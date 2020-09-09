import copy
import json
import logging
import random
import string
import subprocess
import traceback
import unittest
import time

import logger
import testconstants
from TestInput import TestInputSingleton, TestInputServer
from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.documentgenerator import DocumentGenerator


class DockerTestBase(unittest.TestCase):
    suite_setup_done = False
    def setUp(self):
        start = time.time()
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.nodes_init = self.input.param("nodes_init", 2)
        self.master = self.servers[0]
        #self.master.ip = "localhost"
        self.populate_yml_file = "populated.yml"
        self.log.info("Bringing up the images now")
        docker_compose_cmd = "docker-compose -f %s up -d" % \
                             self.populate_yml_file
        subprocess.run(docker_compose_cmd, capture_output=True,
                       shell=True)
        self.rest = RestConnection(self.master)
        self.cluster = Cluster()
        default_params = self._create_bucket_params(
            server=self.master, size=100)
        self.buckets = []
        self.buckets.append(Bucket(name="default",
                                   authType="sasl",
                                   saslPassword="",
                                   num_replicas=default_params[
                                       'replicas'],
                                   bucket_size=100,
                                   eviction_policy=default_params[
                                       'eviction_policy'],
                                   lww=default_params['lww'],
                                   type=default_params[
                                       'bucket_type']))
        end = time.time()
        self.log.info("Time for test setup: {}".format(end - start))

    def tearDown(self):
        self.log.info("Tearing down the images now.")
        docker_compose_cmd = "docker-compose -f %s down" % \
                             self.populate_yml_file
        subprocess.run(docker_compose_cmd, capture_output=True,
                       shell=True)

    def suite_setUp(self):
        start = time.time()
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.nodes_init = self.input.param("nodes_init", 2)
        self.base_file_yml = 'baseline.yml'
        self.base_image_name = self.input.param("base_image",
                                                "couchdata:baseline")
        base_images = ["couchdata:baseline", "couchdata:baseline",
                       "couchdata7.0:baseline", "couchdata7.0:baseline"]
        #for i in range(0, self.servers.__len__()):
        #    base_images.append(self.base_image_name)
        self.create_yml_file(file_name=self.base_file_yml,
                             image=base_images,
                             servers=self.servers)
        docker_compose_cmd = "docker-compose -f %s up -d" % self.base_file_yml
        subprocess.run(docker_compose_cmd, capture_output=True,
                       shell=True)
        self.master = self.servers[0]
        #self.master.ip = "localhost"
        self.rest = RestConnection(self.master)
        self.log.info("Initialising base cluster")
        self.rest.init_cluster(username=self.master.rest_username,
                               password=self.master.rest_password)
        known_nodes = ["ns_1@{}".format(self.servers[0].ip)]
        for i in range(1, self.nodes_init):
            ip = self.servers[i].ip
            otp_node = self.rest.add_node(
                user=self.master.rest_username,
                             password=self.master.rest_password,
                               remoteIp=ip)
            known_nodes.append(otp_node.id)
        self.rest.rebalance(known_nodes)
        rebalance_status = self.rest.monitorRebalance()
        if not rebalance_status:
            self.fail("Rebalance failed. Check logs")
        #time.sleep(2)
        self.cluster = Cluster()
        default_params = self._create_bucket_params(
            server=self.master, size=100)
        self.rest.create_bucket("default", ramQuotaMB=100)
        time.sleep(10)
        self.cluster.create_default_bucket(default_params)
        self.buckets = []
        self.buckets.append(Bucket(name="default",
                                       authType="sasl",
                                       saslPassword="",
                                       num_replicas=default_params[
                                           'replicas'],
                                       bucket_size=100,
                                       eviction_policy=default_params['eviction_policy'],
                                       lww=default_params['lww'],
                                       type=default_params[
                                           'bucket_type']))
        age = list(range(5))
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen = DocumentGenerator('test_docs', template, age, first,
                                start=0,
                                end=1000)
        bucket = self.buckets[0]
        #self.log.info("Loading travel-sample app")
        #self.rest.load_sample("travel-sample")
        self._load_bucket(bucket, self.master, gen, "create", 0)
        self.log.info("Sleeping for 10 sec to let the cluster "
                      "stabilise")
        time.sleep(10)
        docker_compose_cmd = "docker-compose -f %s stop" % self.base_file_yml
        subprocess.run(docker_compose_cmd, capture_output=True,
                       shell=True)
        nodes = ['master']
        for i in range(1, self.servers.__len__()):
            nodes.append("node{}".format(i))
        self.populated_image_name = "couchdata:15m"
        for node in nodes:
            docker_commit_cmd = "docker commit $(docker-compose -f  " \
                                "%s ps -q %s | awk '{print " \
                                "$1}') %s_%s" % (self.base_file_yml,
                                                 node,
                                                 self.populated_image_name, node)
            subprocess.run(docker_commit_cmd, capture_output=True,
                           shell=True)
        docker_compose_cmd = "docker-compose -f %s down" % \
                             self.base_file_yml
        subprocess.run(docker_compose_cmd, capture_output=True,
                       shell=True)
        self.populate_yml_file = "populated.yml"
        populated_yml_images = []
        for node in nodes:
            populated_yml_images.append("%s_%s" % (
                self.populated_image_name, node))
        self.create_yml_file(file_name=self.populate_yml_file,
                             image=populated_yml_images,
                             servers=self.servers)
        end = time.time()
        self.log.info("Time for suite setup : {0}".format(end - start))
        DockerTestBase.suite_setup_done = True

    def suite_tearDown(self):
        docker_compose_cmd = "docker-compose -f %s down" % \
                             self.populate_yml_file
        subprocess.run(docker_compose_cmd, capture_output=True,
                       shell=True)

    def create_yml_file(self, file_name="baseline",
                        image=None, servers=None,
                        with_master=True, different_ports=None):
        if different_ports is None:
            different_ports = []
        if image is None:
            image = ["couchdata:baseline"]
        if servers is None:
            servers = []
        with open(file_name, 'w+') as yml_file:
            yml_file.writelines("version: \"2\"\n")
            yml_file.writelines("services:\n")
            if with_master:
                with open("yml/master.yml", 'r') as master_yml:
                    master_yml_template = master_yml.read()
                    master = servers[0]
                    master_yml_template = master_yml_template.replace(
                        "<image>", image[0])
                    master_yml_template = master_yml_template.replace(
                        "<ip>", master.ip)
                    yml_file.write(master_yml_template)
            with open("yml/node.yml", "r") as node_yml:
                node_template = node_yml.read()
                different_ports_ip = [node.ip for node in different_ports]
                for i in range(1, servers.__len__()):
                    server = servers[i]
                    if server.ip in different_ports_ip:
                        with open("yml/node_with_ports.yml") as \
                                different_ports_yml:
                            temp_node_template = different_ports_yml.read()
                    else:
                        temp_node_template = node_template
                    temp_node_template = temp_node_template.replace(
                        "<number>", i.__str__())
                    temp_node_template = temp_node_template.replace("<image>", image[i])
                    temp_node_template = temp_node_template.replace("<ip>", server.ip)
                    yml_file.write(temp_node_template)
            with open("yml/network.yml", 'r+') as network_yml:
                network_yml_template = network_yml.read()
                yml_file.writelines("networks:\n")
                yml_file.write(network_yml_template)

    def _create_bucket_params(self, server, replicas=1, size=0, port=11211, password=None,
                              bucket_type='membase', enable_replica_index=1, eviction_policy='valueOnly',
                              bucket_priority=None, flush_enabled=1, lww=False, maxttl=None,
                              compression_mode='passive'):
        """Create a set of bucket_parameters to be sent to all of the bucket_creation methods
        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            port - The port to create this bucket on. (String)
            password - The password for this bucket. (String)
            size - The size of the bucket to be created. (int)
            enable_replica_index - can be 0 or 1, 1 enables indexing of replica bucket data (int)
            replicas - The number of replicas for this bucket. (int)
            eviction_policy - The eviction policy for the bucket (String). Can be
                ephemeral bucket: noEviction or nruEviction
                non-ephemeral bucket: valueOnly or fullEviction.
            bucket_priority - The priority of the bucket:either none, low, or high. (String)
            bucket_type - The type of bucket. (String)
            flushEnabled - Enable or Disable the flush functionality of the bucket. (int)
            lww = determine the conflict resolution type of the bucket. (Boolean)

        Returns:
            bucket_params - A dictionary containing the parameters needed to create a bucket."""

        bucket_params = dict()
        bucket_params['server'] = server or self.master
        bucket_params['replicas'] = replicas
        bucket_params['size'] = size
        bucket_params['port'] = port
        bucket_params['password'] = password
        bucket_params['bucket_type'] = bucket_type
        bucket_params['enable_replica_index'] = enable_replica_index
        bucket_params['eviction_policy'] = eviction_policy
        bucket_params['bucket_priority'] = bucket_priority
        bucket_params['flush_enabled'] = flush_enabled
        bucket_params['lww'] = lww
        bucket_params['maxTTL'] = maxttl
        bucket_params['compressionMode'] = compression_mode
        return bucket_params


    def _async_load_bucket(self, bucket, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True,
                           batch_size=1000, pause_secs=1, timeout_secs=30, scope=None, collection=None):
        gen = copy.deepcopy(kv_gen)
        task = self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                bucket.kvs[kv_store], op_type,
                                                exp, flag, only_store_hash,
                                                batch_size, pause_secs, timeout_secs,
                                                compression=True,
                                                scope=scope, collection=collection)
        return task

    def _load_bucket(self, bucket, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True,
                     batch_size=1000, pause_secs=1, timeout_secs=30, scope=None, collection=None):
        task = self._async_load_bucket(bucket, server, kv_gen, op_type, exp, kv_store, flag, only_store_hash,
                                       batch_size, pause_secs, timeout_secs, scope=scope, collection=collection)
        task.result()
