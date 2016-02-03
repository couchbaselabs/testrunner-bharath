import json
from fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection
from lib.membase.api.exception import FTSException, ServerUnavailableException


class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()

    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def check_fts_service_started(self):
        try:
            rest = RestConnection(self._cb_cluster.get_random_fts_node())
            rest.get_fts_index_definition("invalid_index")
        except ServerUnavailableException as e:
            raise FTSException("FTS service has not started: %s" %e)

    def create_simple_default_index(self):
        plan_params = self.construct_plan_params()
        self.load_data()
        self.create_default_indexes_all_buckets(plan_params=plan_params)
        if self._update or self._delete:
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True,
                                      zero_rows_ok=False)
            self.async_perform_update_delete(self.upd_del_fields)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def run_default_index_query(self, query=None, expected_hits=None):
        self.create_simple_default_index()
        zero_results_ok = True
        if not expected_hits:
            expected_hits = int(self._input.param("expected_hits", 0))
            if expected_hits:
                zero_results_ok = False
        if not query:
            query = eval(self._input.param("query", str(self.sample_query)))
            if isinstance(query, str):
                query = json.loads(query)
            zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _ = index.execute_query(query,
                                             zero_results_ok=zero_results_ok,
                                             expected_hits=expected_hits)
            self.log.info("Hits: %s" % hits)

    def test_query_type(self):
        """
        uses RQG
        """
        self.load_data()
        index = self.create_default_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.run_query_and_compare(index)

    def test_query_type_on_alias(self):
        """
        uses RQG
        """
        self.load_data()
        index = self.create_default_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        alias = self.create_alias([index])
        self.generate_random_queries(alias, self.num_queries, self.query_types)
        self.run_query_and_compare(alias)

    def test_match_all(self):
        self.run_default_index_query(query={"match_all": {}},
                                     expected_hits=self._num_items)

    def test_match_none(self):
        self.run_default_index_query(query={"match_none": {}},
                                     expected_hits=0)

    def index_utf16_dataset(self):
        self.load_utf16_data()
        try:
            bucket = self._cb_cluster.get_bucket_by_name('default')
            index = self.create_default_index(bucket, "default_index")
            # an exception will most likely be thrown from waiting
            self.wait_for_indexing_complete()
            self.validate_index_count(
                equal_bucket_doc_count=False,
                zero_rows_ok=True,
                must_equal=0)
        except Exception as e:
            raise FTSException("Exception thrown in utf-16 test :{0}".format(e))

    def create_simple_alias(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _ = index.execute_query(self.sample_query,
                                     zero_results_ok=False)
        alias = self.create_alias([index])
        hits2, _, _ = alias.execute_query(self.sample_query,
                                      zero_results_ok=False)
        if hits != hits2:
            self.fail("Index query yields {0} hits while alias on same index "
                      "yields only {1} hits".format(hits, hits2))
        return index, alias

    def create_query_alias_on_multiple_indexes(self):

        #delete default bucket
        self._cb_cluster.delete_bucket("default")

        # create "emp" bucket
        self._cb_cluster.create_standard_buckets(bucket_size=1000,
                                                 name="emp",
                                                 port=11234,
                                                 num_replicas=0)
        emp = self._cb_cluster.get_bucket_by_name('emp')

        # create "wiki" bucket
        self._cb_cluster.create_standard_buckets(bucket_size=1000,
                                                 name="wiki",
                                                 port=11235,
                                                 num_replicas=0)
        wiki = self._cb_cluster.get_bucket_by_name('wiki')

        #load emp dataset into emp bucket
        emp_gen = self.get_generator(dataset="emp", num_items=self._num_items)
        wiki_gen = self.get_generator(dataset="wiki", num_items=self._num_items)
        if self.es:
            # make deep copies of the generators
            import copy
            emp_gen_copy = copy.deepcopy(emp_gen)
            wiki_gen_copy = copy.deepcopy(wiki_gen)

        load_tasks = self._cb_cluster.async_load_bucket_from_generator(
            bucket=emp,
            kv_gen=emp_gen)
        load_tasks += self._cb_cluster.async_load_bucket_from_generator(
            bucket=wiki,
            kv_gen=wiki_gen)

        if self.es:
            # create empty ES indexes
            self.es.create_empty_index("emp_es_index")
            self.es.create_empty_index("wiki_es_index")
            load_tasks.append(self.es.async_bulk_load_ES(index_name='emp_es_index',
                                                        gen=emp_gen_copy,
                                                        op_type='create'))

            load_tasks.append(self.es.async_bulk_load_ES(index_name='wiki_es_index',
                                                        gen=wiki_gen_copy,
                                                        op_type='create'))

        for task in load_tasks:
            task.result()

        # create indexes on both buckets
        emp_index = self.create_default_index(emp, "emp_index")
        wiki_index = self.create_default_index(
            wiki,
            "wiki_index",
            index_params={"default_analyzer": "simple"})

        self.wait_for_indexing_complete()

        # create compound alias
        alias = self.create_alias(target_indexes=[emp_index, wiki_index],
                                  name="emp_wiki_alias")
        if self.es:
            self.es.create_alias(name="emp_wiki_es_alias",
                                 indexes= ["emp_es_index", "wiki_es_index"])

        # run rqg on the alias
        self.generate_random_queries(alias, self.num_queries, self.query_types)
        self.run_query_and_compare(alias, es_index_name="emp_wiki_es_alias")

    def index_wiki(self):
        self.load_wiki(lang=self.lang)
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "wiki_index")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)

    def delete_index_then_query(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self._cb_cluster.delete_fts_index(index.name)
        try:
            hits2, _, _ = index.execute_query(self.sample_query)
        except Exception as e:
            # expected, pass test
            self.log.error(" Expected exception: {0}".format(e))

    def drop_bucket_check_index(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self._cb_cluster.delete_bucket("default")
        self.sleep(60, "waiting for bucket deletion to be known by fts")
        status, _ = index.get_index_defn()
        if status:
            self.fail("Able to retrieve index json from index "
                      "built on bucket that was deleted")

    def delete_index_having_alias(self):
        index, alias = self.create_simple_alias()
        self._cb_cluster.delete_fts_index(index.name)
        try:
            hits, _, _ = index.execute_query(self.sample_query)
            if hits != 0:
                self.fail("Query alias with deleted target returns query results!")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def create_alias_on_deleted_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        from fts_base import INDEX_DEFAULTS
        alias_def = INDEX_DEFAULTS.ALIAS_DEFINITION
        alias_def['targets'][index.name] = {}
        alias_def['targets'][index.name]['indexUUID'] = index.get_uuid()
        index.delete()
        try:
            self.create_alias([index], alias_def)
            self.fail("Was able to create alias on deleted target")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def edit_index_new_name(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, 'sample_index')
        self.wait_for_indexing_complete()
        index.name = "new_index"
        try:
            index.update()
        except Exception as e:
            self.log.info("Expected exception: {0}".format(e))

    def edit_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, 'sample_index')
        self.wait_for_indexing_complete()
        #hits, _, _ = index.execute_query(self.sample_query)
        new_plan_param = {"maxPartitionsPerPIndex": 30}
        self.partitions_per_pindex = 30
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        defn = index.get_index_defn()
        self.log.info(defn)

    def edit_index_negative(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, 'sample_index')
        self.wait_for_indexing_complete()
        hits, _, _ = index.execute_query(self.sample_query)
        new_plan_param = {"maxPartitionsPerPIndex": 30}
        self.partitions_per_pindex = 30
        index.index_definition['params'] = \
            index.build_custom_index_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        defn = index.get_index_defn()
        self.log.info(defn)

