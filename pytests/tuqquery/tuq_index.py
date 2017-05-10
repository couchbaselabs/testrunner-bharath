import math
import time
import uuid
from tuq import QueryTests
from tuq import ExplainPlanHelper
from tuq_join import JoinTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError

class QueriesViewsTests(QueryTests):

    FIELDS_TO_INDEX = ['name', 'job_title', 'join_yr']
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']

    def setUp(self):
        super(QueriesViewsTests, self).setUp()
        self.num_indexes = self.input.param('num_indexes', 1)
        if self.num_indexes > len(self.FIELDS_TO_INDEX):
            self.input.test_params["stop-on-failure"] = True
            self.log.error("MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED")
            self.fail('MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED')
        self.log.info('-'*100)
        self.log.info('Temp fix for MB-16888')
        self.log.info('-'*100)

        self.shell.execute_command("killall -9 cbq-engine")
        self.shell.execute_command("killall -9 indexes")
        self.sleep(60, 'wait for indexer, cbq processes to come back up ..')
        self.log.info('-'*100)

    def suite_setUp(self):
        super(QueriesViewsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesViewsTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            created_indexes = []
            self.log.info('Temp fix for create index failures MB-16888')
            self.sleep(30, 'sleep before create indexes .. ')
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                                            view_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self._wait_for_index_online(bucket, view_name)
                    if self.index_type == 'VIEW':
                        self._verify_view_is_present(view_name, bucket)
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, view_name, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])

    def test_primary_create_delete_index(self):
        for bucket in self.buckets:
            self.query = "DROP PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])
            self.query = "CREATE PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "tuq_index_%s%s" % (bucket.name, ind)
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (view_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.test_case()
            except Exception, ex:
                if self.index_type == 'VIEW':
                    content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale" : "ok"},
                                                      bucket="default", retry_time=1)
                    self.log.info("Generated view has %s items" % len(content['rows']))
                    raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, view_name, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                self.test_case()

    def test_create_same_name(self):
        for bucket in self.buckets:
            view_name = "tuq_index_%s%s" % (bucket.name, 'VMs')
            try:
                self.query = "CREATE INDEX %s ON %s(%s) USING VIEW" % (view_name, bucket.name, 'VMs')
                actual_result = self.run_cbq_query()
                self._verify_results(actual_result['results'], [])
                self.query = "CREATE INDEX %s ON %s(%s) USING GSI" % (view_name, bucket.name, 'VMs')
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, view_name)
                self._verify_results(actual_result['results'], [])
            finally:
                try:
                    self.query = "DROP INDEX %s.%s USING VIEW" % (bucket.name, view_name)
                    self.run_cbq_query()
                    self.query = "DROP INDEX %s.%s USING GSI" % (bucket.name, view_name)
                    self.run_cbq_query()
                except:
                    pass

    '''MB-22129: Test that the created index coveries the queries using LET and LETTING'''
    def test_explain_let_letting(self):
        idx = "idx_bc"
        self.query = 'CREATE INDEX %s ON default( join_mo, join_yr) ' % idx
        self.run_cbq_query()

        # Test let
        # Number of expected hits for the select query
        result_count = 504
        self.query = "EXPLAIN SELECT d, e FROM default LET d = join_mo, e = join_yr " \
                     "WHERE d > 11 AND e > 2010"
        result = self.run_cbq_query()
        plan = ExplainPlanHelper(result)
        self.query = "SELECT d, e FROM default LET d = join_mo, e = join_yr " \
                     "WHERE d > 11 AND e > 2010"
        result = self.run_cbq_query()
        self.assertTrue(plan['~children'][0]['index'] == idx
                        and 'join_mo' in plan['~children'][0]['covers'][0]
                        and 'join_yr' in plan['~children'][0]['covers'][1]
                        and result['metrics']['resultCount'] == result_count)

        # Test letting
        result_count = 2
        self.query = 'EXPLAIN SELECT d, e FROM default LET d = join_mo ' \
                     'WHERE d > 10 GROUP BY d LETTING e = SUM(join_yr) HAVING e > 20'
        result = self.run_cbq_query()
        plan = ExplainPlanHelper(result)
        self.query = 'SELECT d, e FROM default LET d = join_mo ' \
                     'WHERE d > 10 GROUP BY d LETTING e = SUM(join_yr) HAVING e > 20'
        result = self.run_cbq_query()
        self.assertTrue(plan['~children'][0]['index'] == idx
                        and 'join_mo' in plan['~children'][0]['covers'][0]
                        and 'join_yr' in plan['~children'][0]['covers'][1]
                        and result['metrics']['resultCount'] == result_count)

        self.query = "DROP INDEX default.%s USING %s" % (idx,self.index_type)

    '''MB-22148: The span produced by an OR predicate should be variable in length'''
    def test_variable_length_sarging_or(self):
        idx = "idx_ab"
        result_count = 468
        self.query = 'CREATE INDEX %s ON default( join_day, join_mo) ' % idx
        self.run_cbq_query()

        self.query = "EXPLAIN SELECT * FROM default " \
                     "WHERE join_day = 5 OR ( join_day = 10 AND join_mo = 10 )"
        result = self.run_cbq_query()
        plan = ExplainPlanHelper(result)
        self.query = "SELECT * FROM default WHERE join_day = 5 OR ( join_day = 10 AND join_mo = 10 )"
        result = self.run_cbq_query()
        self.assertTrue(result['metrics']['resultCount'] == result_count)
        self.assertTrue(len(plan['~children'][0]['scans'][1]['spans'][0]['range']) == 2)

        self.query = "DROP INDEX default.%s USING %s" % (idx, self.index_type)

    '''MB-22111: Unnest array covering indexes should not have DistinctScan unless a Distinct array
       is being used in the index'''
    def test_unnest_covering_array_index(self):
        idx = "by_VMs"
        self.query = 'CREATE INDEX %s ON default (ALL ARRAY r.`name` FOR r IN VMs END, email)' % idx
        self.run_cbq_query()

        result_count = 3024
        self.query = 'explain SELECT t.email, r.`name` FROM default t UNNEST t.VMs AS r ' \
                     'WHERE r.`name` IN [ "vm_12", "vm_13" ]'
        result = self.run_cbq_query()
        plan = ExplainPlanHelper(result)
        self.query = 'SELECT t.email, r.`name` FROM default t UNNEST t.VMs AS r ' \
                     'WHERE r.`name` IN [ "vm_12", "vm_13" ]'
        query_result = self.run_cbq_query()
        # plan.values()[1][0].values() is where DistinctScan would appear if it exists
        self.assertTrue("DistinctScan" not in plan.values()[1][0].values()
                        and query_result['metrics']['resultCount'] == result_count)

        result_count = 2016
        self.query = 'explain SELECT t.email, r.`name` FROM default t UNNEST t.VMs AS r ' \
                     'WHERE r.`name` = "vm_12"'
        result = self.run_cbq_query()
        plan2 = ExplainPlanHelper(result)
        self.query = 'SELECT t.email, r.`name` FROM default t UNNEST t.VMs AS r ' \
                     'WHERE r.`name` = "vm_12"'
        query_result2 = self.run_cbq_query()
        # plan.values()[1][0].values() is where DistinctScan would appear if it exists
        self.assertTrue("DistinctScan" not in plan2.values()[1][0].values()
                        and query_result2['metrics']['resultCount'] == result_count)
        self.query = "DROP INDEX default.%s USING %s" % (idx, self.index_type)
        self.run_cbq_query()

        idx2 = "by_VMs2"
        self.query = 'CREATE INDEX %s ON ' \
                     'default (DISTINCT ARRAY r.`name` FOR r IN VMs END,VMs, email)' % idx2
        self.run_cbq_query()

        self.query = 'explain SELECT t.email, r.`name` FROM default t UNNEST t.VMs AS r ' \
                     'WHERE r.`name` = "vm_12"'
        result = self.run_cbq_query()
        plan3 = ExplainPlanHelper(result)
        self.query = 'SELECT t.email, r.`name` FROM default t UNNEST t.VMs AS r ' \
                     'WHERE r.`name` = "vm_12"'
        query_result3 = self.run_cbq_query()
        # Since DistinctScan does exist we can just look for its specific key
        self.assertTrue("DistinctScan" in plan3.values()[1][0].values()
                        and plan3['~children'][0]['#operator'] == "DistinctScan"
                        and query_result3['metrics']['resultCount'] == result_count)
        self.query = "DROP INDEX default.%s USING %s" % (idx2, self.index_type)
        self.run_cbq_query()

    def test_explain(self):
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT * FROM %s" % (bucket.name)
            res = self.run_cbq_query()
            self.log.info(res)
	    plan = ExplainPlanHelper(res)
            self.assertTrue(plan["~children"][0]["index"] == "#primary",
                            "Type should be #primary, but is: %s" % plan)

            self.query = "EXPLAIN SELECT * FROM %s LIMIT %s" %(bucket.name,10);
            res = self.run_cbq_query()
            self.assertTrue('limit' in str(res['results']),
                            "Limit is not pushed to primary scan")

    def test_explain_query_count(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s ' % (bucket.name)
                res = self.run_cbq_query()
                self.log.info(res)
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_group_by(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s GROUP BY VMs' % (bucket.name)
                res = self.run_cbq_query()
                self.log.info(res)
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_array(self):
        for bucket in self.buckets:
            index_name = "my_index_arr"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT ARRAY vm.memory FOR vm IN VMs END AS vm_memories FROM %s' % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(meta(%s).type) USING %s" % (index_name, bucket.name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s WHERE meta(%s).type = "json"' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_push_limit_intersect_unionscan(self):
      created_indexes = []
      try:
        self.query = "create index ix1 on default(join_day,VMs[0].os)"
        self.run_cbq_query()
        created_indexes.append("ix1")
        self.query = "create index ix2 on default(VMs[0].os)"
        self.run_cbq_query()
        created_indexes.append("ix2")
        self.query = "create index ix3 on default(VMs[0].memory) where VMs[0].memory > 10"
        self.run_cbq_query()
        created_indexes.append("ix3")
        self.query = "explain select * from default where join_day > 10 AND VMs[0].os = 'ubuntu' LIMIT 10"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("limit" in plan['~children'][0]['~children'][0])

        self.query = "explain select * from default where join_day > 10 AND VMs[0].memory > 10"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("covers" not in str(plan))
        self.query = "explain select join_day from default where join_day > 10 AND VMs[0].memory > 10"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("cover" not in str(plan))
        self.query = "select join_day from default where join_day > 10 AND VMs[0].memory > 10 order by meta().id"
        expected_result = self.run_cbq_query()
        self.query = "create index ix4 on default(VMs[0].memory,join_day) where VMs[0].memory > 10"
        self.run_cbq_query()
        created_indexes.append("ix4")
        self.query = "explain select join_day from default where join_day > 10 AND VMs[0].memory > 10"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("cover" in str(plan))
        self.query = "select join_day from default where join_day > 10 AND VMs[0].memory > 10 order by meta().id"
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])
        self.query = "select join_day from default use index(`#primary`) where join_day > 10 AND VMs[0].memory > 10 order by meta().id"
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])
        self.query = "select * from default where join_day > 10 AND VMs[0].os = 'ubuntu' LIMIT 10"
        res = self.run_cbq_query()
        self.assertTrue(res['metrics']['resultCount']==10)
        self.query = "explain select * from default where join_day > 10 OR VMs[0].os = 'ubuntu'"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("cover" not in str(plan))
        self.query = "explain select join_day from default where join_day > 10 OR VMs[0].memory > 10"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("cover" in str(plan))
        self.query = "explain select join_day from default where join_day > 10 OR VMs[0].os = 'ubuntu'"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("cover" not  in str(plan))
        #self.assertEquals(plan['~children'][0]['~children'][0]['limit'],'10')
        self.query = "select * from default where join_day > 10 OR VMs[0].os = 'ubuntu' LIMIT 10"
        res = self.run_cbq_query()
        self.assertTrue(res['metrics']['resultCount']==10)
        self.query = "explain select * from default where join_day > 10 and VMs[0].memory > 0 and VMs[0].os = 'ubuntu' LIMIT 10"
        res = self.run_cbq_query()
        plan = ExplainPlanHelper(res)
        self.assertTrue("limit" not in plan['~children'][0]['~children'][0])
        self.query = "select * from default where join_day > 10 and VMs[0].memory > 0 and VMs[0].os = 'ubuntu' LIMIT 10"
        res = self.run_cbq_query()
        self.assertTrue(res['metrics']['resultCount']==10)
      finally:
        for idx in created_indexes:
            self.query = "DROP INDEX %s.%s USING %s" % ("default", idx, self.index_type)
            self.run_cbq_query()

    def test_meta_no_duplicate_results(self):
        self.query = 'insert into default values ("k01",{"name":"abc"})'
        self.run_cbq_query()
        self.query = 'select name,meta().id from default where meta().id IN ["k01",""]'
        res = self.run_cbq_query()
        self.assertTrue(res['results']==[{u'id': u'k01', u'name': u'abc'}])
        self.query = 'delete from default use keys ["k01"]'
        self.run_cbq_query()

    def test_unnest_when(self):
        created_indexes = []
        for bucket in self.buckets:
            try:
                idx1 = "unnest_idx"
                idx2 = "idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i.memory FOR i in %s  when i.memory > 10 END) " % (
                    idx1, bucket.name, "VMs")
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx1)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx1)
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i.memory FOR i in %s END) " % (
                    idx2, bucket.name, "VMs")
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx1)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx1), "Index is not in list")
                self.query = "EXPLAIN select %s.name from %s UNNEST VMs as x where any i in default.VMs satisfies i.memory > 9 END" % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                result1 =plan['~children'][0]['scan']['index']
                self.assertTrue(result1==idx2)
                self.query = "EXPLAIN select %s.name from %s UNNEST VMs as x where any i in default.VMs satisfies i.memory > 10 END" % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                result1 =plan['~children'][0]['scans'][0]['scan']['index']
                self.assertTrue(result1==idx1)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % ("default", idx, self.index_type)
                    self.run_cbq_query()



    def test_notin_notwithin(self):
      created_indexes = []
      try:
        idx = "ix"
        self.query = 'create index {0} on default(join_day)'.format(idx)
        self.run_cbq_query()
        self.query = 'explain select 1 from default where NOT (join_day IN [ 1])'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['scan']['index'] ==idx)
        self.query = 'explain select 1 from default where NOT (join_day WITHIN [ 1])'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['index'] ==idx)
        self.query = 'explain select 1 from default where (join_day IN NOT [ 1])'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['index']==idx)
        self.query = 'explain select 1 from default where (join_day WITHIN NOT [ 1])'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['index']==idx)
        self.query = 'explain select 1 from default where join_day NOT WITHIN [ 1]'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['index'] ==idx)
        self.query = 'explain select 1 from default where join_day NOT IN [ 1]'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['scan']['index'] ==idx)
      finally:
        for idx in created_indexes:
            self.query = "DROP INDEX %s.%s USING %s" % ("default", idx, self.index_type)
            self.run_cbq_query()

    def test_create_arrays_ranging_over_object(self):
        self.query = 'select array j for i:j in {"a":1, "b":2} end'
        res = self.run_cbq_query()
        self.assertTrue(res['results']==[{u'$1': [1, 2]}])
        self.query = 'select array j for i:j in {"a":1, "b":2, "c":[2,3], "d": "%s", "e":2, "f": %s } end'%("verbose",'{"a":1}')
        res = self.run_cbq_query()
        self.assertTrue(res['results']==[{u'$1': [1, 2, [2, 3], u'verbose', 2, {u'a': 1}]}])

    '''MB-21011: Explain queries should not run the query that they generate the explain plan for'''
    def test_explain_prepared(self):
        self.run_cbq_query("EXPLAIN prepare a from select * from default")
        prepareds = self.run_cbq_query("SELECT * from system:prepareds")
        self.assertTrue(prepareds['metrics']['resultCount'] == 0)

    def test_explain_index_with_fn(self):
        for bucket in self.buckets:
            index_name = "my_index_fn"
            try:
                self.query = "CREATE INDEX %s ON %s(round(test_rate)) USING %s" % (index_name, bucket.name, bucket.name, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN select name, round(test_rate) as rate from %s WHERE round(test_rate) = 2' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()


    def test_explain_index_attr(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_attr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    self.query = "EXPLAIN SELECT * FROM %s WHERE %s = 'abc'" % (bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
                    created_indexes.append(index_name)
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_non_index_attr(self):
        for bucket in self.buckets:
            index_name = "my_non_index"
            try:
                self.query = "CREATE INDEX %s ON %s(name) USING %s" % (index_name, bucket.name, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = "EXPLAIN SELECT * FROM %s WHERE email = 'abc'" % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] != index_name,
                                "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_index_count_gn(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT COUNT(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_aggr_gn(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT SUM(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_join(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name) USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee JOIN %s as new_task" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_unnest(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(tasks_ids) USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT emp.name, task FROM %s emp UNNEST emp.tasks_ids task" % (bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_subquery(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_day) USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from %s use keys ['query-1'] where join_day>2) as names from %s" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs FROM %s ' % (bucket.name) + \
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 AND vm.os = "ubuntu" end'
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE tasks_points > 0'
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects_element(self):
        for bucket in self.buckets:
            index_name = "my_index_obj_el"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE tasks_points.task1 > 0'
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list_element(self):
        for bucket in self.buckets:
            index_name = "my_index_list_el"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                         ' FROM %s WHERE skills[0] = "abc"' % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list(self):
        for bucket in self.buckets:
            index_name = "my_index_list"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                         ' FROM %s WHERE skill[0] = "skill2010"' % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_several_complex_objects(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_index_complex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'EXPLAIN SELECT DISTINCT %s as complex FROM %s WHERE %s = "abc"' % (self.FIELDS_TO_INDEX[ind - 1],
                                                                                                      bucket.name,
                                                                                                      self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_index_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
               self.query = "CREATE INDEX %s ON %s(" % (index_name, bucket.name) + \
               "meta(%s"%(bucket.name) + ").id) USING %s" % self.index_type
               # if self.gsi_type:
               #     self.query += " WITH {'index_type': 'memdb'}"
               self.run_cbq_query()
            except Exception, ex:
               self.assertTrue(str(ex).find("Error creating index") != -1,
                              "Error message is %s." % str(ex))
            else:
                self.fail("Error message expected")
    
    def test_index_dates(self):
        for bucket in self.buckets:
            index_name = "my_index_date"
            try:
                self.query = "CREATE INDEX %s ON %s(" % (index_name, bucket.name) + \
                "str_to_millis(tostr(join_yr) || '-0' || tostr(join_mo) || '-' || tostr(join_day))) "
                self.run_cbq_query()
            except Exception, ex:
                self.assertTrue(str(ex).find("Error creating index") != -1,
                                "Error message is %s." % str(ex))
            else:
                self.fail("Error message expected")

    def test_multiple_index_hints_explain_select(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo', 'join_day,join_mo']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                    bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % (ind_name))
                for ind in created_indexes:
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s  USE INDEX(%s using %s) WHERE join_day>2 AND join_mo>3' % (bucket.name, ind, self.index_type)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_index_hints_explain_aggr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['job_title', 'test_rate', 'job_title,test_rate']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                    bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % (ind_name))
                for ind in created_indexes:
                    self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s as employees USE INDEX(%s using %s)" % (bucket.name, ind, self.index_type) +\
                                 " WHERE job_title='Sales' GROUP BY join_mo " +\
                                 "HAVING SUM(employees.test_rate) > 0 and " +\
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
                    self.assertTrue(res["status"] == 'success')
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_index_hints_explain_same_attr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                fields = ['job_title', 'job_title,test_rate']
                for attr in fields:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (ind_name,
                                                                       bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % (ind_name))
                for ind in created_indexes:
                    self.query = "EXPLAIN SELECT join_mo, SUM(test_rate) as rate FROM %s  as employees USE INDEX(%s using %s)" % (bucket.name, ind, self.index_type) +\
                                 "WHERE job_title='Sales' GROUP BY join_mo " +\
                                 "HAVING SUM(employees.test_rate) > 0 and " +\
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_indexes_query_attr(self):
        index_name_prefix = 'auto_ind'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_day,join_mo']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s) " % (ind_name,
                                                                    bucket.name, attr)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append(ind_name)
                    self.query = 'SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (bucket.name)
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name" : doc['name'], "join_mo" : doc['join_mo'], "join_day" : doc["join_day"]}
                                       for doc in full_list if doc['join_day'] > 2 and doc['join_mo'] > 3]
                    #import pdb;pdb.set_trace()
                    self.query = "select * from %s" % bucket.name
                    self.run_cbq_query()
                    self._verify_results(sorted(res['results']), sorted(expected_result))
                    #self.assertTrue(len(res['results'])==0)
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind_name,
                                    "Index should be %s, but is: %s" % (ind_name, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_indexes_query_non_ind_attr(self):
        index_name_prefix = 'auto_ind'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo']:
                    index_name = '%s_%s%s' % (index_name_prefix, attr, str(uuid.uuid4())[:4])
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name,
                                                                bucket.name, attr)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name" : doc['name'], "join_yr" : doc['join_yr'], "join_day" : doc["join_day"]}
                                       for doc in full_list if doc['join_yr'] > 3]
                    #import pdb;pdb.set_trace()
                    self._verify_results(sorted(res['results']), sorted(expected_result))
                    #self.assertTrue(len(res['results'])==10)
                    self.query = 'EXPLAIN SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] != '%s_%s' % (index_name_prefix, attr),
                                    "Index should be %s_%s, but is: %s" % (index_name_prefix, attr, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_negative_indexes(self):
        queries_errors = {'create index gsi on default(name) using gsi': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_prepared_with_index_simple_where(self):
        index_name_prefix = 'auto_ind_prepared'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo']:
                    index_name = '%s_%s' % (index_name_prefix, attr)
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name,
                                                                    bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append('%s_%s' % (index_name_prefix, attr))
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    self.prepared_common_body()
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_run_query(self):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_any')
        index_fields = self.input.param("index_field", '').split(';')
        index_name = "test"
        for bucket in self.buckets:
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
            except:
                for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
                raise
        try:
            self.query = "select * from system:indexes where name = %s" % (index_name)
            self.log.info(self.run_cbq_query())
            self.hint_index = indexes[0]
            fn = getattr(self, method_name)
            fn()
        finally:
            for bucket in self.buckets:
                for indx in indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                        try:
                            self.run_cbq_query()
                        except:
                            pass

    def test_prepared_hints_letting(self):
        self.run_query_prepared("SELECT join_mo, sum_test from {0} USE INDEX ({1} using {2}) WHERE join_mo>7 group by join_mo letting sum_test = sum(tasks_points.task1)")

    def test_prepared_hints_array(self):
        self.run_query_prepared("SELECT job_title, array_append(array_agg(DISTINCT name), 'new_name') as names FROM {0} USE INDEX ({1} using {2}) GROUP BY job_title" )

    def test_prepared_hints_intersect(self):
        self.run_query_prepared("select name from {0} intersect all select name from {0} s USE INDEX ({1} using {2}) where s.join_day>5")

    def run_query_prepared(self, query):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        index_fields = self.input.param("index_field", '').split(';')
        for bucket in self.buckets:
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
            except:
                for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
                raise
        try:
            self.query = "select * from system:indexes where name = %s" % (index_name)
            self.log.info(self.run_cbq_query())
            for bucket in self.buckets:
                self.query = query.format(bucket.name, indexes[0], self.index_type)
                self.prepared_common_body()
        finally:
            for bucket in self.buckets:
                for indx in indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                        try:
                            self.run_cbq_query()
                        except:
                            pass

    def test_intersect_scan(self):
        test_to_run = self.input.param("test_to_run", '')
        indexes = []
        try:
            indexes, query = self.run_intersect_scan_query(test_to_run)
            self.run_intersect_scan_explain_query(indexes, query)
        finally:
            if indexes:
                  for bucket in self.buckets:
                    for indx in indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                        try:
                            self.run_cbq_query()
                        except:
                            pass

    def test_intersect_scan_meta(self):
        test_to_run = self.input.param("test_to_run", '')
        indexes = []
        try:
            indexes = []
            index_name_prefix = "inter_index_" + str(uuid.uuid4())[:4]
            index_fields = self.input.param("index_field", '').split(';')
            for bucket in self.buckets:
                for field in index_fields:
                    index_name = '%sid_meta' % (index_name_prefix)
                    query = "CREATE INDEX %s ON %s(meta(%s).id) USING %s" % (
                        index_name, bucket.name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                    index_name = '%stype_meta' % (index_name_prefix)
                    query = "CREATE INDEX %s ON %s(meta(%s).type) USING %s" % (
                        index_name, bucket.name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                self.test_comparition_meta()
            for bucket in self.buckets:
                self.query = "SELECT meta(%s).id, meta(%s).type FROM %s" % (bucket.name, bucket.name, bucket.name)
                self.run_cbq_query()
                query = 'EXPLAIN ' % (self.query % (bucket.name, bucket.name, bucket.name))
                res = self.run_cbq_query(query=query)
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["~children"][0]["#operator"] == 'IntersectScan',
                                        "Index should be intersect scan and is %s" % (plan))
                actual_indexes = [scan['index'] for scan in plan["~children"][0]["~children"][0]['scans']]
                self.assertTrue(set(actual_indexes) == set(indexes),
                                "Indexes should be %s, but are: %s" % (indexes, actual_indexes))
        finally:
            for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def run_intersect_scan_query(self, query_method):
        indexes = []
        query = None
        index_name_prefix = "inter_index_" + str(uuid.uuid4())[:4]
        index_fields = self.input.param("index_field", '').split(';')
        try:
            for bucket in self.buckets:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                    index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                fn = getattr(self, query_method)
                query = fn()
        finally:
            return indexes, query

    def run_intersect_scan_explain_query(self, indexes_names, query_temp):
        for bucket in self.buckets:
            if (query_temp.find('%s') > 0):
                query_temp = query_temp % bucket.name
            query = 'EXPLAIN %s' % (query_temp)
            res = self.run_cbq_query(query=query)

	    plan = ExplainPlanHelper(res)
            print plan
            self.log.info('-'*100)
            if (query.find("CREATE INDEX") < 0):
                result = plan["~children"][0]["~children"][0] if "~children" in plan["~children"][0] \
                        else plan["~children"][0]
                print result
                #import pdb;pdb.set_trace()
                if not(result['scans'][0]['#operator']=='DistinctScan'):
                    if not (result["#operator"] == 'UnionScan'):
                        self.assertTrue(result["#operator"] == 'IntersectScan',
                                        "Index should be intersect scan and is %s" % (plan))
                    # actual_indexes = []
                    # for scan in result['scans']:
                    #     print scan
                    #     if (scan['#operator'] == 'IndexScan'):
                    #         actual_indexes.append([result['scans'][0]['index']])
                    #
                    #     elif (scan['#operator'] == 'DistinctScan'):
                    #         actual_indexes.append([result['scans'][0]['scan']['index']])
                    #     else:
                    #          actual_indexes.append(scan['index'])
                    if result["#operator"] == 'UnionScan':
                        actual_indexes = [scan['index'] if scan['#operator'] == 'IndexScan' else scan['scan']['index'] if scan['#operator'] == 'DistinctScan' else scan['index']
                                          for results in result['scans'] for scan in results['scans']]
                    else:
                        actual_indexes = [scan['index'] if scan['#operator'] == 'IndexScan' else scan['scan']['index'] if scan['#operator'] == 'DistinctScan' else scan['index']
                                for scan in result['scans']]

                    print actual_indexes

                    actual_indexes = [x.encode('UTF8') for x in actual_indexes]

                    self.log.info('actual indexes "{0}"'.format(actual_indexes))
                    self.log.info('compared against "{0}"'.format(indexes_names))
                    self.assertTrue(set(actual_indexes) == set(indexes_names),"Indexes should be %s, but are: %s" % (indexes_names, actual_indexes))
            else:
                result = plan
                self.assertTrue(result['#operator'] == 'CreateIndex',
                                    "Operator is not create index and is %s" % (result))
            self.log.info('-'*100)

    def _delete_indexes(self, indexes):
        count = 0
        for bucket in self.buckets:
                query = "DROP INDEX %s.%s USING %s" % (bucket.name, indexes[count], self.index_type)
                count =count+1
                try:
                   self.run_cbq_query(query=query)
                except:
                   pass

    def _verify_view_is_present(self, view_name, bucket):
        if self.primary_indx_type.lower() == 'gsi':
            return
        ddoc, _ = RestConnection(self.master).get_ddoc(bucket.name, "ddl_%s" % view_name)
        self.assertTrue(view_name in ddoc["views"], "View %s wasn't created" % view_name)

    def _is_index_in_list(self, bucket, index_name):
        query = "SELECT * FROM system:indexes"
        res = self.run_cbq_query(query)
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                self.log.error(item)
                continue
            if item['indexes']['keyspace_id'] == bucket.name and item['indexes']['name'] == index_name:
                return True
        return False

class QueriesJoinViewsTests(JoinTests):


    def setUp(self):
        super(QueriesJoinViewsTests, self).setUp()
        self.num_indexes = self.input.param('num_indexes', 1)
        self.index_type = self.input.param('index_type', 'VIEW')

    def suite_setUp(self):
        super(QueriesJoinViewsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesJoinViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesJoinViewsTests, self).suite_tearDown()

    def test_run_query(self):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_simple_join_keys')
        index_fields = self.input.param("index_field", '').split(';')
        for bucket in self.buckets:
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                fn = getattr(self, method_name)
                fn()
            finally:
                for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
