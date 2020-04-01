import os as OS
import subprocess
import sys

from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

try:
    import requests
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "requests"])
    import requests
import argparse

host = '172.23.121.84'
bucket_name = 'rerun_jobs'


def get_params(url):
    """
    Get parameters from the jenkins job
    @param url: The jenkins job URL
    @type url: str
    @return: Dictionary of job parameters
    @rtype: dict
    """
    res = get_js(url, params="tree=actions[parameters[*]]")
    parameters = {}
    if not res:
        print "Error: could not get parameters"
        exit(0)
    for vals in res['actions']:
        if "parameters" in vals:
            for params in vals['parameters']:
                parameters[params['name']] = params['value']
            break
    return parameters


def get_js(url, params=None):
    """
    Get the parameters from Jenkins job using Jenkins rest api
    @param url: The jenkins job URL
    @type url: str
    @param params: Parameters to be passed to the json/api
    @type params: str
    @return: Response from the rest api
    @rtype: dict
    """
    res = None
    try:
        res = requests.get("%s/%s" % (url, "api/json"),
                           params=params, timeout=15)
        data = res.json()
        return data
    except:
        print "Error: url unreachable: %s" % url
        exit(0)
    return res


def get_run_results():
    run_results = {}
    return run_results


def parse_args():
    """
    Parse command line arguments into a dictionary
    @return: Dictionary of parsed command line arguments
    @rtype: dict
    """
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("build_version", type=str,
                                 help="Couchbase build version of the "
                                      "job")
    argument_parser.add_argument("--executor_jenkins_job",
                                 action='store_true',
                                 help="Run with current executor job")
    argument_parser.add_argument("--jenkins_job", action="store_true",
                                 help="Run with current jenkins job")
    argument_parser.add_argument("--store_data", action="store_true",
                                 help="Store the test_run details. To "
                                      "be used only after testrunner "
                                      "is run")
    argument_parser.add_argument("--install_failure",
                                 action='store_true',
                                 help="Was there install failure in "
                                      "the run?")
    args = vars(argument_parser.parse_args())
    return args


def build_args(build_version, executor_jenkins_job=False,
               jenkins_job=False, store_data=False,
               install_failure=False):
    """
    Build a dictionary of arguments needed for the program
    @param build_version: Couchbase build version of the job
    @type build_version: str
    @param executor_jenkins_job: Run with current Executor job
    @type executor_jenkins_job: bool
    @param jenkins_job: Run with current jenkins job
    @type jenkins_job: bool
    @param store_data: "Store the test_run details. To be used only
    after testrunner is run"
    @type store_data: bool
    @param install_failure: Was there install failure in the run?
    @type install_failure: bool
    @return: Dictionary of parameters
    @rtype: dict
    """
    return locals()


def find_rerun_job(args):
    """
    Find if the job was run previously
    @param args: Dictionary of arguments. Run build_args() if calling
    this from python script or parse_args() if running from shell
    @type args: dict
    @return: If the job was run previously
    @rtype: bool
    """
    name = None
    store_data = args['store_data']
    install_failure = args['install_failure']
    if args['executor_jenkins_job']:
        os = OS.getenv('os')
        component = OS.getenv('component')
        sub_component = OS.getenv('subcomponent')
        version_build = OS.getenv('version_number')
        name = "{}_{}_{}".format(os, component, sub_component)
    elif args['jenkins_job']:
        name = OS.getenv('JOB_NAME')
        version_build = args['build_version']
    else:
        os = args['os']
        component = args['component']
        sub_component = args['sub_component']
        if os and component and sub_component:
            name = "{}_{}_{}".format(os, component, sub_component)
        elif args['name']:
            name = args['name']
        version_build = args['build_version']
    if not name or not version_build:
        return False, {}
    cluster = Cluster('couchbase://{}'.format(host))
    authenticator = PasswordAuthenticator('Administrator', 'password')
    cluster.authenticate(authenticator)
    rerun_jobs = cluster.open_bucket(bucket_name)
    rerun = False
    doc_id = "{}_{}".format(name, version_build)
    try:
        run_document = rerun_jobs.get(doc_id, quiet=True)
        if not store_data:
            if not run_document.success:
                return False, {}
            else:
                return True, run_document.value
        parameters = get_params(OS.getenv('BUILD_URL'))
        run_results = get_run_results()
        job_to_store = {
            "job_url": OS.getenv('BUILD_URL'),
            "build_id": OS.getenv('BUILD_ID'),
            "run_params": parameters,
            "run_results": run_results,
            "install_failure": install_failure}
        if run_document.success:
            rerun = True
            run_document = run_document.value
        else:
            run_document = {
                "build": version_build,
                "num_runs": 0,
                "jobs": []}
        run_document['num_runs'] += 1
        run_document['jobs'].append(job_to_store)
        rerun_jobs.upsert(doc_id, run_document, ttl=(7*24*60*60))
        return rerun, run_document
    except Exception as e:
        print e
        return False, {}


if __name__ == "__main__":
    args = parse_args()
    rerun, document = find_rerun_job(args)
    print rerun.__str__()