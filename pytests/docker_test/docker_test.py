import time

from docker_test.docker_test_base import DockerTestBase
from membase.api.rest_client import RestConnection


class DockerTest(DockerTestBase):

    def setUp(self):
        if(DockerTestBase.suite_setup_done):
            super(DockerTest, self).setUp()

    def suite_setUp(self):
        super(DockerTest, self).suite_setUp()

    def suite_tearDown(self):
        super(DockerTest, self).suite_tearDown()

    def test_1(self):
        servers = self.servers[self.nodes_init:]
        self.rest = RestConnection(self.master)
        for server in servers:
            otp_node = self.rest.add_node(server.rest_username,
                               server.rest_password, server.ip)
        known_nodes = self.rest.node_statuses()
        known_nodes_ids = [node.id for node in known_nodes]
        remove_node_ips = [node.ip for node in self.servers[:self.nodes_init]]
        remove_nodes_ids = [node.id for node in known_nodes if
                            node.ip in remove_node_ips ]
        self.rest.rebalance(known_nodes_ids, remove_nodes_ids)
        self.rest.monitorRebalance()
        time.sleep(60)

    def test_2(self):
        servers = self.servers[self.nodes_init:]
        self.rest = RestConnection(self.master)
        for server in servers:
            otp_node = self.rest.add_node(server.rest_username,
                                          server.rest_password,
                                          server.ip)
        known_nodes = self.rest.node_statuses()
        known_nodes_ids = [node.id for node in known_nodes]
        remove_node_ips = [node.ip for node in
                           self.servers[:self.nodes_init]]
        remove_nodes_ids = [node.id for node in known_nodes if
                            node.ip in remove_node_ips]
        self.rest.rebalance(known_nodes_ids, remove_nodes_ids)
        self.rest.monitorRebalance()
        time.sleep(60)