# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import cStringIO
import gzip
import json
import os
import urlparse
import sys
from collections import defaultdict
from pprint import pprint

import requests

from mozlog import reader

treeherder_base = "https://treeherder.mozilla.org/"

"""Simple script for downloading structured logs from treeherder.

For the moment this is specialised to work with web-platform-tests
logs; in due course it should move somewhere generic and get hooked
up to mach or similar"""

# Interpretation of the "job" list from
# https://github.com/mozilla/treeherder-service/blob/master/treeherder/webapp/api/utils.py#L18

JOB_NAMES = {
    "Mochitest e10s Browser Chrome": ("mochitest-bc", True),
    "Mochitest e10s Browser DevTools Chrome": ("mochitest-devtools", True),
    "Mochitest e10s Other": ("mochitest-other", True),
    "Mochitest e10s WebGL": ("mochitest-gl", True),
    "Mochitest e10s": ("mochitest-plain", True),
    "Mochitest Browser Chrome": ("mochitest-bc", False),
    "Mochitest Browser DevTools Chrome": ("mochitest-devtools", False),
    "Mochitest Other": ("mochitest-other", False),
    "Mochitest WebGL": ("mochitest-gl", False),
    "Mochitest": ("mochitest-plain", False),
    "W3C Web Platform Reftests e10s": ("wpt-reftest", True),
    "W3C Web Platform Tests e10s": ("wpt", True),
    "W3C Web Platform Reftests": ("wpt-reftest", False),
    "W3C Web Platform Tests": ("wpt", False),
    # These don't have structured logs yet
    # "Crashtest e10s": ("crashtest", True),
    # "Crashtest": ("crashtest", False),
    # "JSReftest e10s": ("jsreftest", True),
    # "JSReftest": ("jsreftest", False),
    # "Reftest e10s": ("reftest", True),
    # "Reftest": ("reftest", False),
}

def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--format",
                        action="store",
                        choices=["json", "text", "html"],
                        default="json",
                        help="Output format")
    parser.add_argument("-o", "--output",
                        action="store",
                        type=argparse.FileType('w'),
                        default=sys.stdout,
                        help="Output file")
    parser.add_argument("load", action="store",
                        nargs="+", help="Either two arguments: branch name, commit sha1 to load from treeherder, or one argument, the path to json output file to load")
    return parser

def get_file(url):
    return cStringIO.StringIO(requests.get(url).content)

def get_blobber_urls(branch, job):
    job_id = job["id"]
    resp = requests.get(urlparse.urljoin(treeherder_base,
                                         "/api/project/%s/artifact/?job_id=%i&name=Job%%20Info" % (branch,
                                                                                                   job_id)))
    job_data = resp.json()

    if job_data:
        assert len(job_data) == 1
        job_data = job_data[0]
        try:
            details = job_data["blob"]["job_details"]
            urls = [item["url"] for item in details if item["value"].endswith("_raw.log")]
            return urls
        except:
            return None

def get_job_results(branch, commit):
    resp = requests.get(urlparse.urljoin(treeherder_base, "/api/project/%s/resultset/?revision=%s" % (branch, commit)))

    revision_data = resp.json()

    result_set = revision_data["results"][0]["id"]

    resp = requests.get(urlparse.urljoin(treeherder_base, "/api/project/%s/jobs/?result_set_id=%s&count=2000&exclusion_profile=false" % (branch, result_set)))

    return resp.json()["results"]

def group_results_by_type(job_data):
    results_by_type = defaultdict(lambda: defaultdict(lambda: [[], []]))

    for result in job_data:
        job_type_name = result["job_type_name"]
        if job_type_name in JOB_NAMES:
            platform_id = result["platform"]
            if result["platform_option"]:
                platform_id += " " + result["platform_option"]
            category, e10s = JOB_NAMES[job_type_name]
            results_by_type[category][platform_id][int(e10s)].append(result)

    return results_by_type

class ResultHandler(reader.LogHandler):
    def __init__(self):
        self.data = {}
        self.result = None

    def test_end(self, data):
        key = (data["test"], None)
        self._insert(key, data["status"])

    def test_status(self, data):
        key = (data["test"], data["subtest"])
        self._insert(key, data["status"])

    def _insert(self, key, status):
        if key in self.data:
            idx = int(key[1] is not None)

            old_key = key

            # Attempt to possibly generate a unique key where there is a duplicate.
            # We have no way to be sure that this will work across runs since ordering may
            # not be consistent
            i = 0
            while True:
                i += 1
                possibility = key[idx] + " - %s" % i
                new_key = [item for item in key]
                new_key[idx] = possibility
                new_key = tuple(new_key)
                if new_key not in self.data:
                    key = new_key
                    break

            sys.stderr.write("WARNING - Duplicate key %s in %s, using %s\n" % (old_key, self.result["ref_data_name"], new_key))

        self.data[key] = (status, self.result)

def load_results(branch, results):
    handler = ResultHandler()
    for result in results:
        urls = get_blobber_urls(branch, result)
        if urls:
            prefix = result["platform"] # platform
            for url in urls:
                f = get_file(url)
                handler.result = result
                reader.handle_log(reader.read(f), handler)
    return {key: value[0] for key, value in handler.data.iteritems()}

def compare_results(non_e10s, e10s):
    differences = {}

    for key, value in e10s.iteritems():
        if not key in non_e10s:
            # don't add subtests unless the parent test wasn't disabled
            if key[1] is None or non_e10s.get((key[0], None), "").lower() != "skip":
                differences[key] = [None, value]
        elif non_e10s[key] != value:
            differences[key] = [non_e10s[key], value]

    for key, value in non_e10s.iteritems():
        if not key in e10s:
            if key[1] is None or e10s.get((key[0], None), "").lower() != "skip":
                differences[key] = [value, None]

    return differences

def group_by_test(by_platform):
    by_test = defaultdict(lambda: defaultdict(list))
    for platform, differences in by_platform.iteritems():
        for test, results in sorted(differences.iteritems()):
            by_test[test][tuple(results)].append(platform)

    return by_test

class Output(object):
    def __init__(self, branch, commit, dest):
        self.dest = dest
        self.branch = branch
        self.commit = commit

    def start(self):
        pass

    def write(self, job_type_name, by_platform):
        raise NotImplementedError

    def end(self):
        pass

class HTMLOutput(Output):
    head = """<!doctype html>
<title>e10s differences</title>\n"""

    table_start = """<table>
<tr>
  <th>Test</th>
  <th>Subtest</th>
  <th>non-e10s Result</th>
  <th>e10s Result</th>
  <th>Platforms</th>
</tr>\n"""

    table_end = """</table>\n"""

    def start(self):
        self.dest.write(self.head)
        self.dest.write("""<h1>e10s differences from %(branch)s commit %(commit)s</h1>
        <p><a href="https://treeherder.mozilla.org/#/jobs?repo=%(branch)s&revision=%(commit)s">Treeherder</a></p>\n""" % {"branch": self.branch, "commit": self.commit})

    def write(self, job_type_name, by_platform):
        self.dest.write("<h2>%s</h2>\n" % job_type_name)

        if not by_platform:
            self.dest.write("<p>No logs found</p>\n")
            return

        if not any(item for item in by_platform.itervalues()):
            self.dest.write("<p>No differences found</p>\n")
            return

        by_test = group_by_test(by_platform)

        first_row_template = """<tr>
  <td rowspan="%(num_results)s">%(test)s
  <td rowspan="%(num_results)s">%(subtest)s
  <td>%(result_non_e10s)s
  <td>%(result_e10s)s
  <td>%(platforms)s
</tr>\n"""

        latter_row_template = """<tr>
  <td>%(result_non_e10s)s
  <td>%(result_e10s)s
  <td>%(platforms)s
</tr>\n"""

        self.dest.write(self.table_start)
        for (test, subtest), results in sorted(by_test.iteritems()):
            for i, ((non_e10s, e10s), platforms) in enumerate(results.iteritems()):
                data = {"test": test.encode("utf-8"),
                        "subtest": subtest.encode("utf-8") if subtest is not None else "",
                        "result_non_e10s": self.format_result(non_e10s),
                        "result_e10s": self.format_result(e10s),
                        "num_results": len(results),
                        "platforms": ", ".join(item.encode("utf-8") for item in platforms)}
                template = first_row_template if i == 0 else latter_row_template
                self.dest.write(template % data)

        self.dest.write(self.table_end)

    def end(self):
        pass

    def format_result(self, result):
        if result is None:
            return "&lt;missing>"
        return result.title().encode("utf-8")

class TextOutput(Output):
    def write(self, job_type_name, by_platform):
        self.dest.write("%s\n%s\n" % (job_type_name, "=" * len(job_type_name)))

        if not by_platform:
            self.dest.write("No logs found\n")
            return

        if not any(item for item in by_platform.itervalues()):
            self.dest.write("No differences found\n")
            return

        by_test = group_by_test(by_platform)

        row_template = """%(test)s | %(subtest)s | %(result_non_e10s)s | %(result_e10s)s | %(platforms)s\n"""

        for (test, subtest), results in sorted(by_test.iteritems()):
            for i, ((non_e10s, e10s), platforms) in enumerate(results.iteritems()):
                data = {"test": test.encode("utf-8"),
                        "subtest": subtest.encode("utf-8") if subtest is not None else "",
                        "result_non_e10s": self.format_result(non_e10s),
                        "result_e10s": self.format_result(e10s),
                        "num_results": len(results),
                        "platforms": ", ".join(item.encode("utf-8") for item in platforms)}
                self.dest.write(row_template % data)
            self.dest.write("\n")

    def format_result(self, result):
        if result is None:
            return "<missing>"
        return result.title().encode("utf-8")

class JSONOutput(Output):
    def __init__(self, branch, commit, dest):
        Output.__init__(self, branch, commit, dest)
        self.data = {"branch": branch,
                     "commit": commit,
                     "differences": {}}

    def write(self, job_type_name, by_platform):
        self.data["differences"][job_type_name] = {key: list(value.iteritems())
                                                   for key, value in by_platform.iteritems()}

    def end(self):
        json.dump(self.data, self.dest)

def output_file(input_file, output_cls, dest=sys.stdout):
    data = json.load(input_file)
    for job_type_name, by_platform in data["differences"].iteritems():
        for platform, differences in by_platform.iteritems():
            by_platform[platform] = dict((tuple(item[0]), item[1]) for item in differences)
    branch = data["branch"]
    commit = data["commit"]

    output = output_cls(branch, commit, dest)

    output.start()
    for job_type_name, by_platform in sorted(data["differences"].iteritems()):
        output.write(job_type_name, by_platform)
    output.end()

def compare(branch, commit, output_cls, dest=sys.stdout):
    output = output_cls(branch, commit, dest)

    results_data = get_job_results(branch, commit)
    results_by_type = group_results_by_type(results_data)

    output.start()

    for job_type_name, results_by_platform in sorted(results_by_type.iteritems()):
        by_platform = {}
        for platform, results in results_by_platform.iteritems():
            print >> sys.stderr, job_type_name, platform, [len(item) for item in results]
            if any(len(item) == 0 for item in results):
                by_platform[platform] = {}
                continue
            test_results = [load_results(branch, item) for item in results]
            if any(item for item in test_results):
                by_platform[platform] = compare_results(*test_results)
        output.write(job_type_name, by_platform)

    output.end()

def main():
    parser = create_parser()
    args = parser.parse_args()
    if len(args.load) == 1:
        try:
            input_file = open(args.load[0])
        except IOError:
            parser.exit("Failed to read %s" % path)
    elif len(args.load) == 2:
        input_file = None
        branch, commit = args.load
    else:
        parser.error("At most two positional arguments may be supplied")

    output_cls = {"json": JSONOutput,
                  "text": TextOutput,
                  "html": HTMLOutput}[args.format]

    if input_file:
        return output_file(input_file, output_cls, args.output)
    else:
        return compare(branch, commit, output_cls, args.output)

if __name__ == "__main__":
    import pdb
    import traceback
    try:
        main()
    except Exception:
        print >> sys.stderr, traceback.format_exc()
        pdb.post_mortem()
