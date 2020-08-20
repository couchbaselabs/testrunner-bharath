import subprocess
import sys
import docker
import re

version_number = sys.argv[1]
version = version_number.split('-')[0]
build_number = version_number.split('-')[1]
flavor = sys.argv[2]
os = sys.argv[3]

docker_location = "docker/ubuntu"
#docker_location = "/Users/bharathgp/sequoia/containers/couchbase"
#if os == "centos":
#    docker_location = "{0}/CentOS7".format(docker_location)
#else:
#    docker_location = "{0}/Ubuntu20".format(docker_location)
docker_file_location = "{}/Dockerfile".format(docker_location)
with open(docker_file_location, 'r') as docker_file:
    file_data = docker_file.read()

file_data = re.sub("VERSION=.*", "VERSION={}".format(version),
                   file_data)
file_data = re.sub("BUILD_NO=.*", "BUILD_NO={}".format(build_number),
                   file_data)
file_data = re.sub("FLAVOR=.*", "FLAVOR={}".format(flavor), file_data)
#file_data = file_data.replace("VERSION=5.0.0", "VERSION={}".format(
# version))
#file_data = file_data.replace("BUILD_NO=2412", "BUILD_NO={}".format(
#    build_number))
#file_data = file_data.replace("FLAVOR=spock", "FLAVOR={}".format(
#    flavor))
with open(docker_file_location, 'w') as docker_file:
    docker_file.write(file_data)

#docker_build_command = "docker build {0} -t couchdata:baseline"
client = docker.from_env()
response = [line for line in client.images.build(
    path=docker_location, tag="couchdata:baseline")]
print(response)