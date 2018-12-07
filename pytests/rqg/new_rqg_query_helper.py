import random
import copy
import string
import pprint
from base_query_helper import BaseRQGQueryHelper

'''
N1QL PARSE ORDER
1.  with
2.  from
3.  let
4.  where
5.  group
6.  letting
7.  having
8.  select
9.  order by
10. offset
11. limit

RQG RESERVED KEYWORD LIST

*CLAUSE KEYWORD*
WITH_START
WITH_END
FROM_START
FROM_END
LET_START
LET_END
WHERE_START
WHERE_END
GROUPBY_START
GROUPBY_END
LETTING_START
LETTING_END
HAVING_START
HAVING_END
SELECT_START
SELECT_END
ORDERBY_START
ORDERBY_END
OFFSET_START
OFFSET_END
LIMIT_START
LIMIT_END

*INTERNAL KEYWORDS*
CTE_START
CTE_END
WITH_CLAUSE_SUBQUERY
NESTED_WITH_CLAUSE_SUBQUERY
CHAINED_WITH_CLAUSE_SUBQUERY
WITH_CLAUSE_CONSTANT
CTE_ALIAS
WITH_CLAUSE_ALIAS
FIELDS
FIELDS_CONDITION
FROM_FIELD
FROM_CLAUSE
LEFT OUTER JOIN
RIGHT OUTER JOIN
INNER JOIN
WITH_TEMPLATE
WITH_EXPRESSION_TEMPLATES
WITH_EXPRESSION_ORDER
WITH_EXPRESSIONS
WITH_FIELDS
WITH_CLAUSE
FROM_TEMPLATE
BUCKET_NAME
TABLE_AND_CTE_JOIN
TABLE_CTE
TABLE_TABLE
CTE_CTE
CTE_TABLE
WHERE_CLAUSE
'''


class RQGQueryHelperNew(BaseRQGQueryHelper):

    ''' Dispatcher function. Uses test_name parameter to identify the way how templates
        will be transformed into SQL and N1QL queries.
        from let where group by letting haVING SELECT ORDER BY'''
    def _get_conversion_func(self, test_name):
        if test_name == 'group_by_alias':
            return self._convert_sql_template_for_group_by_aliases
        elif test_name == 'skip_range_key_scan':
            return self._convert_sql_template_for_skip_range_scan
        elif test_name == 'common_table_expression':
            return self._convert_sql_template_for_common_table_expression
        else:
            print("Unknown test name")
            exit(1)

    def log_info(self, object):
        if not self.debug_logging:
            return
        pprint.pprint(object)

    def _convert_sql_template_for_common_table_expression(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        template_map = self._extract_clauses(query_template)
        template_map = self._convert_with_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map)
        template_map["N1QL"] = self._combine_converted_clauses(template_map)
        template_map = self.convert_on_clause_for_sql(template_map)
        template_map["SQL"] = self._combine_converted_clauses(template_map)
        indexes = {}
        indexes = self.create_join_index(conversion_map, template_map, indexes)
        query_map = {"n1ql": template_map['N1QL'],  "sql": template_map['SQL'],
                     "bucket": str(",".join(table_map.keys())),
                     "expected_result": None, "indexes": indexes,
                     "tests": ["BASIC"]}
        query_map = self.convert_table_name(query_map, conversion_map)
        return query_map

    def convert_on_clause_for_sql(self, template_map):
        from_map = template_map['FROM_FIELD']
        from_type = from_map['type']
        if from_type == "joins":
            from_clause = template_map['FROM_CLAUSE']
            on_clause = from_clause.split(" ON ")[1].strip("(").strip(")").replace("==", "=")
            from_clause = from_clause.split(" ON ")[0] + " ON " + on_clause
            template_map['FROM_CLAUSE'] = from_clause
        return template_map

    def create_join_index(self, conversion_map, template_map, indexes={}):
        table_name = conversion_map.get("table_name", "simple_table")
        from_map = template_map['FROM_FIELD']
        from_type = from_map['type']
        if from_type == "joins":
            join_type = from_map['join_type']
            random_index_name = "join_index_" + str(self._random_int())
            if join_type == "LEFT OUTER JOIN":
                statement = "create index " + random_index_name + " on " + table_name + "(" + from_map['right_on_field'] + ")"
                indexes[random_index_name] = {"name": random_index_name, "type": "GSI", "definition": statement}
            elif join_type == "RIGHT OUTER JOIN" or join_type == "INNER JOIN":
                statement = "create index " + random_index_name + " on " + table_name + "(" + from_map['left_on_field'] + ")"
                indexes[random_index_name] = {"name": random_index_name, "type": "GSI", "definition": statement}
            else:
                pass
        return indexes

    def _extract_clauses(self, query_template):
        with_sep = ("WITH_TEMPLATE", "WITH_START", "WITH_END")
        from_sep = ("FROM_TEMPLATE", "FROM_START", "FROM_END")
        let_sep = ("LET_TEMPLATE", "LET_START", "LET_END")
        where_sep = ("WHERE_TEMPLATE", "WHERE_START", "WHERE_END")
        groupby_sep = ("GROUPBY_TEMPLATE", "GROUPBY_START", "GROUPBY_END")
        letting_sep = ("LETTING_TEMPLATE", "LETTING_START", "LETTING_END")
        having_sep = ("HAVING_TEMPLATE", "HAVING_START", "HAVING_END")
        select_sep = ("SELECT_TEMPLATE", "SELECT_START", "SELECT_END")
        orderby_sep = ("ORDERBY_TEMPLATE", "ORDERBY_START", "ORDERBY_END")
        offset_sep = ("OFFSET_TEMPLATE", "OFFSET_START", "OFFSET_END")
        limit_sep = ("LIMIT_TEMPLATE", "LIMIT_START", "LIMIT_END")

        clause_seperators = [with_sep, from_sep, let_sep, where_sep, groupby_sep,
                             letting_sep, having_sep, select_sep, orderby_sep, offset_sep, limit_sep]
        parsed_clauses = dict()
        parsed_clauses['RAW_QUERY_TEMPLATE'] = query_template
        for clause_seperator in clause_seperators:
            clause = clause_seperator[0]
            start_sep = clause_seperator[1]
            end_sep = clause_seperator[2]
            result = []
            tmp = query_template.split(start_sep)
            for substring in tmp:
                if end_sep in substring:
                    result.append(substring.split(end_sep)[0].strip())
            parsed_clauses[clause] = result
        return parsed_clauses

    def _convert_with_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        with_template = self.get_from_dict(template_map, key_path+['WITH_TEMPLATE', 0])
        self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES'], {})
        self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_ORDER'], {})
        start_sep = "CTE_START"
        end_sep = "CTE_END"
        tmp = with_template.split(start_sep)
        i = 0
        for substring in tmp:
            if end_sep in substring:
                random_alias = 'CTE_' + ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
                clause_template = substring.split(end_sep)[0].strip()
                self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', random_alias], clause_template)
                self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_ORDER', random_alias], i)
                i += 1
        with_ordering = []
        with_expression_ordering_map = self.get_from_dict(template_map, key_path+['WITH_EXPRESSION_ORDER'])
        for with_alias in with_expression_ordering_map.keys():
            with_ordering.append((with_alias, self.get_from_dict(template_map, key_path+['WITH_EXPRESSION_ORDER', with_alias])))

        with_ordering.sort(key=lambda x: x[1])

        self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS'], {})
        self.set_in_dict(template_map, key_path+['WITH_FIELDS'], {})

        for with_order in with_ordering:
            with_expression_alias = with_order[0]
            template_map = self._convert_with_expression(with_expression_alias, template_map, conversion_map, key_path=key_path)
        converted_with_clause = "WITH"
        for with_order in with_ordering:
            converted_with_expression_alias = with_order[0]
            expression = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS', converted_with_expression_alias])
            converted_with_clause += " " + converted_with_expression_alias + " as " + "(" + expression + "),"

        with_clause = str(self._remove_trailing_substring(converted_with_clause.strip(), ","))
        self.set_in_dict(template_map, key_path+['WITH_CLAUSE'], with_clause)
        return template_map

    def _convert_with_expression(self, alias, template_map, conversion_map, key_path=[]):
        expression = self.get_from_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias])
        if expression == "WITH_CLAUSE_SUBQUERY":
            query_template = "SELECT_START SELECT FIELDS SELECT_END FROM_START FROM BUCKET_NAME FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias], self._extract_clauses(query_template))
            template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            with_expression = self._combine_converted_clauses(template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS', alias], with_expression)
            self.set_in_dict(template_map, key_path+['WITH_FIELDS', alias], self._extract_fields_from_clause("SELECT", template_map, conversion_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias]))
        elif expression == "NESTED_WITH_CLAUSE_SUBQUERY":
            query_template = "WITH_START WITH CTE_START WITH_CLAUSE_SUBQUERY CTE_END WITH_END SELECT_START SELECT FIELDS SELECT_END FROM_START FROM WITH_CLAUSE_ALIAS FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias], self._extract_clauses(query_template))
            template_map = self._convert_with_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            with_expression = self._combine_converted_clauses(template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS', alias], with_expression)
            self.set_in_dict(template_map, key_path+['WITH_FIELDS', alias], self._extract_fields_from_clause("SELECT", template_map, conversion_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias]))
        elif expression == "CHAINED_WITH_CLAUSE_SUBQUERY":
            query_template = "SELECT_START SELECT FIELDS SELECT_END FROM_START FROM CTE_ALIAS FROM_END WHERE_START WHERE CTE_FIELDS_CONDITION WHERE_END"
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias], self._extract_clauses(query_template))
            template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            with_expression = self._combine_converted_clauses(template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS', alias], with_expression)
            self.set_in_dict(template_map, key_path+['WITH_FIELDS', alias], self._extract_fields_from_clause("SELECT", template_map, conversion_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias]))
        else:
            print("Unknown with expression template")
            exit(1)
        return template_map

    def _convert_from_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        from_template = self.get_from_dict(template_map, key_path+['FROM_TEMPLATE', 0])
        from_expression = from_template.split("FROM")[1].strip()
        from_clause = "FROM"
        table_name = conversion_map.get("table_name", "simple_table")
        if from_expression == "BUCKET_NAME":
            # any select is from the default table/bucket
            from_clause += " " + table_name
            from_map = {"left_table": table_name, "class": "BUCKET",
                        'type': "basic"}
        elif from_expression == "WITH_CLAUSE_ALIAS":
            # outer most select is from a with clause alias
            with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
            with_clause_aliases = with_clause_aliases.keys()
            with_alias = random.choice(with_clause_aliases)
            from_clause += " " + with_alias
            from_map = {"left_table": with_alias, "class": "WITH_ALIAS", "type": "basic"}
        elif from_expression == "CTE_ALIAS":
            # select in a cte is from a previous cte
            last_index = 0
            i = 0
            for part in key_path:
                if part == 'WITH_EXPRESSION_TEMPLATES':
                    last_index = i
                i += 1
            with_expression_order_key_path = key_path[:last_index]
            with_expression_ordering = self.get_from_dict(template_map, with_expression_order_key_path + ['WITH_EXPRESSION_ORDER'])
            target_alias = key_path[-1]
            target_alias_order = with_expression_ordering[target_alias]
            source_aliases = []
            for source_alias in with_expression_ordering.keys():
                if with_expression_ordering[source_alias] < target_alias_order:
                    source_aliases.append(source_alias)
            if len(source_aliases) == 0:
                print("No with clause to chain to")
                exit(1)
            source_alias = random.choice(source_aliases)
            from_clause += " " + source_alias + " " + "AS" + " " + source_alias+source_alias
            from_map = {"left_table": source_alias, "left_table_alias": source_alias+source_alias,
                        "class": "CTE_ALIAS", "type": "basic"}
        elif from_expression == "TABLE_AND_CTE_JOIN":
            with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
            with_clause_aliases = with_clause_aliases.keys()
            if len(with_clause_aliases) < 2:
                join_order = random.choice(['TABLE_CTE', 'CTE_TABLE', 'TABLE_TABLE'])
            else:
                with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
                with_clause_aliases = with_clause_aliases.keys()
                left_table = random.choice(with_clause_aliases)
                with_clause_aliases.remove(left_table)
                right_table = random.choice(with_clause_aliases)

                left_with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', left_table])
                left_with_alias_fields = [tuple[0] for tuple in left_with_alias_fields]

                right_with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', right_table])
                right_with_alias_fields = [tuple[0] for tuple in right_with_alias_fields]

                common_fields = [field for field in left_with_alias_fields if field in right_with_alias_fields]
                if len(common_fields) == 0:
                    join_order = random.choice(['TABLE_CTE', 'TABLE_TABLE', 'CTE_TABLE'])
                else:
                    join_order = random.choice(['CTE_CTE', 'TABLE_CTE', 'TABLE_TABLE', 'CTE_CTE'])

            if join_order == "TABLE_CTE" or join_order == "CTE_TABLE":
                # select is from a table/bucket joined to a cte
                with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
                with_clause_aliases = with_clause_aliases.keys()
                with_alias = random.choice(with_clause_aliases)
                with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', with_alias])
                with_alias_fields = [tuple[0] for tuple in with_alias_fields]
                join_field = random.choice(with_alias_fields)
                join_type = random.choice(["LEFT OUTER JOIN", "RIGHT OUTER JOIN", "INNER JOIN"])

                if join_order == "CTE_TABLE":
                    left_table = with_alias
                    right_table = table_name

                if join_order == "TABLE_CTE":
                    left_table = table_name
                    right_table = with_alias

                from_clause += " " + left_table + " " + join_type + " " + right_table + " " + "ON" + " " + "(" + left_table + "." + join_field + " " + "==" + " " + right_table + "." + join_field + ")"
                from_map = {"left_table": left_table, "class": "TABLE_AND_CTE_JOIN", "type": "joins",
                            "right_table": right_table, "left_on_field": join_field, "right_on_field": join_field,
                            "join_type": join_type}
            elif join_order == "TABLE_TABLE":
                table_map = conversion_map.get("table_map", {})
                table_fields = table_map[table_name]["fields"].keys()
                join_field = random.choice(table_fields)
                join_type = random.choice(["LEFT OUTER JOIN", "RIGHT OUTER JOIN", "INNER JOIN"])
                left_table = table_name
                left_table_alias = left_table+"_ALIAS_LEFT"
                right_table = table_name
                right_table_alias = right_table+"_ALIAS_RIGHT"
                from_clause += " " + left_table + " AS " + left_table_alias + " " + join_type + " " + right_table + " AS " + right_table_alias + " " + "ON" + " " + "(" + left_table_alias + "." + join_field + " " + "==" + " " + right_table_alias + "." + join_field + ")"
                from_map = {"left_table": left_table, "left_table_alias": left_table_alias, "class": "TABLE_AND_CTE_JOIN", "type": "joins",
                        "right_table": right_table, "right_table_alias": right_table_alias, "left_on_field": join_field, "right_on_field": join_field,
                        "join_type": join_type}
            elif join_order == "CTE_CTE":
                left_table_alias = left_table + "_ALIAS_LEFT"
                right_table_alias = right_table + "_ALIAS_RIGHT"

                join_field = random.choice(common_fields)

                join_type = random.choice(["LEFT OUTER JOIN", "RIGHT OUTER JOIN", "INNER JOIN"])
                from_clause += " " + left_table + " AS " + left_table_alias + " " + join_type + " " + right_table + " AS " + right_table_alias + " " + "ON" + " " + "(" + left_table_alias + "." + join_field + " " + "==" + " " + right_table_alias + "." + join_field + ")"
                from_map = {"left_table": left_table, "left_table_alias": left_table_alias, "class": "TABLE_AND_CTE_JOIN", "type": "joins",
                            "right_table": right_table, "right_table_alias": right_table_alias, "left_on_field": join_field, "right_on_field": join_field,
                            "join_type": join_type}
        else:
            print("Unknown from clause type")
            exit(1)
        self.set_in_dict(template_map, key_path+['FROM_FIELD'], from_map)
        self.set_in_dict(template_map, key_path+['FROM_CLAUSE'], str(from_clause))
        return template_map

    def _convert_where_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        where_clause = "WHERE"
        from_map = self.get_from_dict(template_map, key_path+['FROM_FIELD'])
        from_class = from_map["class"]
        table_map = conversion_map.get("table_map", {})

        num_where_comparisons = random.randint(0, 4)
        if num_where_comparisons == 0:
            where_clause = ""
            self.set_in_dict(template_map, key_path+['WHERE_CLAUSE'], where_clause)
            return template_map

        if from_class == "BUCKET":
            # need to add random field selection from bucket
            from_table = from_map["left_table"]
            all_fields = table_map[from_table]["fields"].keys()

            for i in range(0, num_where_comparisons):
                random_field = random.choice(all_fields)
                random_constant = self._random_constant(random_field)
                comparator = random.choice(['<', '>', '=', '!='])
                conjunction = random.choice(['AND', 'OR'])
                where_clause += " " + random_field + " " + comparator + " " + str(random_constant) + " " + conjunction
            where_clause = where_clause.rsplit(' ', 1)[0]

        elif from_class == "WITH_ALIAS":
            from_table = from_map["left_table"]
            with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', from_table])
            for i in range(0, num_where_comparisons):
                random_with_field_info = random.choice(with_alias_fields)
                random_with_field = random_with_field_info[0]
                random_constant = self._random_constant(random_with_field)
                comparator = random.choice(['<', '>', '=', '!='])
                conjunction = random.choice(['AND', 'OR'])
                where_clause += " " + from_table + "." + random_with_field + " " + comparator + " " + str(random_constant) + " " + conjunction
            where_clause = where_clause.rsplit(' ', 1)[0]

        elif from_class == "CTE_ALIAS":
            from_table = from_map["left_table"]
            from_table_alias = from_map["left_table_alias"]
            last_index = 0
            i = 0
            for part in key_path:
                if part == 'WITH_EXPRESSION_TEMPLATES':
                    last_index = i
                i += 1
            with_fields_key_path = key_path[:last_index]

            with_fields = self.get_from_dict(template_map, with_fields_key_path + ['WITH_FIELDS', from_table])
            source_fields = [field_tuple[0] for field_tuple in with_fields]

            for i in range(0, num_where_comparisons):
                where_field = random.choice(source_fields)
                comparator = random.choice(['<', '>', '=', '!='])
                conjunction = random.choice(['AND', 'OR'])
                random_constant = self._random_constant(where_field)
                where_clause += " " + from_table_alias + "." + where_field + " " + comparator + " " + str(random_constant) + " " + conjunction
            where_clause = where_clause.rsplit(' ', 1)[0]

        elif from_class == "TABLE_AND_CTE_JOIN":
            from_map = self.get_from_dict(template_map, key_path + ['FROM_FIELD'])

            left_table = from_map['left_table']
            left_table_alias = from_map.get('left_table_alias', "NO_ALIAS")
            if left_table_alias == "NO_ALIAS":
                left_table_alias = left_table

            if left_table.startswith("CTE"):
                left_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', left_table])
                left_table_fields = [tuple[0] for tuple in left_table_fields]
            else:
                left_table_fields = table_map[left_table]["fields"].keys()

            right_table = from_map['right_table']
            right_table_alias = from_map.get('right_table_alias', "NO_ALIAS")
            if right_table_alias == "NO_ALIAS":
                right_table_alias = right_table

            if right_table.startswith("CTE"):
                right_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', right_table])
                right_table_fields = [tuple[0] for tuple in right_table_fields]
            else:
                right_table_fields = table_map[right_table]["fields"].keys()

            for i in range(0, num_where_comparisons):
                comparator = random.choice(['<', '>', '=', "!="])
                conjunction = random.choice(['AND', 'OR'])
                if random.choice(["LEFT_TABLE", "RIGHT_TABLE"]) == "LEFT_TABLE":
                    where_field = random.choice(left_table_fields)
                    where_table = left_table_alias
                else:
                    where_field = random.choice(right_table_fields)
                    where_table = right_table_alias
                random_constant = self._random_constant(where_field)
                where_clause += " " + where_table + "." + where_field + " " + comparator + " " + str(random_constant) + " " + conjunction
            where_clause = where_clause.rsplit(' ', 1)[0]

        else:
            print("Unknown from expression type")
            exit(1)

        self.set_in_dict(template_map, key_path+['WHERE_CLAUSE'], where_clause)
        return template_map

    def _convert_select_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        select_template = self.get_from_dict(template_map, key_path+['SELECT_TEMPLATE', 0])
        select_expression = select_template.split("SELECT")[1].strip()
        select_clause = "SELECT"
        from_map = self.get_from_dict(template_map, key_path+['FROM_FIELD'])

        if select_expression == "FIELDS":
            random_select_fields = self._get_random_select_fields(from_map, conversion_map, template_map, key_path=key_path)
        else:
            print("Unknown select type")
            exit(1)

        for field in random_select_fields:
            select_clause += " " + field + ","

        select_clause = self._remove_trailing_substring(select_clause.strip(), ",")
        self.set_in_dict(template_map, key_path+['SELECT_CLAUSE'], select_clause)
        return template_map

    def _combine_converted_clauses(self, template_map, key_path=[]):
        clause_order = ["WITH_CLAUSE", "SELECT_CLAUSE", "FROM_CLAUSE", "LET_CLAUSE", "WHERE_CLAUSE", "GROUPBY_CLAUSE", "LETTING_CLAUSE",
                        "HAVING_CLAUSE", "ORDERBY_CLAUSE", "OFFSET_CLAUSE", "LIMIT_CLAUSE"]
        query = ""
        for clause in clause_order:
            converted_clause = self.get_from_dict(template_map, key_path).get(clause, "")
            if converted_clause != "":
                query += converted_clause + " "
        return query

    """this function takes in a dictionary and a list of keys 
    and will return the value after traversing all the keys in the list
    https://stackoverflow.com/questions/14692690/access-nested-dictionary-items-via-a-list-of-keys"""
    def get_from_dict(self, data_dict, key_list):
        for key in key_list:
            data_dict = data_dict[key]
        return data_dict

    """this function take in a dictionary, a list of keys, and a value
    and will set the value after traversing the list of keys
    https://stackoverflow.com/questions/14692690/access-nested-dictionary-items-via-a-list-of-keys"""
    def set_in_dict(self, data_dict, key_list, value):
        for key in key_list[:-1]:
            data_dict = data_dict.setdefault(key, {})
        data_dict[key_list[-1]] = value

    def _get_random_select_fields(self, from_map, conversion_map, template_map, key_path=[]):
        from_class = from_map['class']
        table_map = conversion_map.get("table_map", {})
        random_fields = []
        if from_class == "BUCKET":
            from_table = from_map['left_table']
            all_fields = table_map[from_table]["fields"].keys()
            random_fields = self._random_sample(all_fields)

        elif from_class == "WITH_ALIAS":
            from_table = from_map['left_table']
            all_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', from_table])
            all_fields = [field_tuple[0] for field_tuple in all_fields]
            if len(all_fields) == 0:
                random_fields = [from_table]
            else:
                random_fields = self._random_sample(all_fields)
                random_fields = [from_table + "." + field for field in random_fields]

        elif from_class == "CTE_ALIAS":
            from_table = from_map['left_table']
            from_table_alias = from_map['left_table_alias']
            last_index = 0
            i = 0
            for part in key_path:
                if part == 'WITH_EXPRESSION_TEMPLATES':
                    last_index = i
                i += 1
            with_fields_key_path = key_path[:last_index]
            with_fields = self.get_from_dict(template_map, with_fields_key_path + ['WITH_FIELDS'])
            target_fields = with_fields[from_table]
            all_fields = [field_tuple[0] for field_tuple in target_fields]
            random_fields = self._random_sample(all_fields)
            random_fields = [from_table_alias + "." + field for field in random_fields]

        elif from_class == "TABLE_AND_CTE_JOIN":
            left_table = from_map['left_table']
            left_table_alias = from_map.get('left_table_alias', "NO_ALIAS")
            if left_table_alias == "NO_ALIAS":
                left_table_alias = left_table

            if left_table.startswith("CTE"):
                left_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', left_table])
                left_table_fields = [(left_table_alias, tuple[0]) for tuple in left_table_fields]
            else:
                left_table_fields = table_map[left_table]["fields"].keys()
                left_table_fields = [(left_table_alias, field) for field in left_table_fields]

            right_table = from_map['right_table']
            right_table_alias = from_map.get('right_table_alias', "NO_ALIAS")
            if right_table_alias == "NO_ALIAS":
                right_table_alias = right_table

            if right_table.startswith("CTE"):
                right_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', right_table])
                right_table_fields = [(right_table_alias, tuple[0]) for tuple in right_table_fields]
            else:
                right_table_fields = table_map[right_table]["fields"].keys()
                right_table_fields = [(right_table_alias, field) for field in right_table_fields]

            all_fields = []
            seen_fields = []
            check_fields = left_table_fields + right_table_fields
            random.shuffle(check_fields)
            for tuple in check_fields:
                if tuple[1] not in seen_fields:
                    seen_fields.append(tuple[1])
                    all_fields.append(tuple)
            random_fields = self._random_sample(all_fields)
            random_fields = [field[0] + "." + field[1] for field in random_fields]

        else:
            print("Unknown from type for select clause conversion")
            exit(1)

        return random_fields

    def convert_table_name(self, query_map, conversion_map):
        database = conversion_map['database_name']
        query_map["n1ql"] = query_map['n1ql'].replace("simple_table", database + "_" + "simple_table")
        for key in query_map['indexes'].keys():
            if 'definition' in query_map['indexes'][key]:
                query_map['indexes'][key]['definition'] = query_map['indexes'][key]['definition'].replace("simple_table", database + "_" + "simple_table")
        return query_map

    def _random_sample(self, list):
        size_of_sample = random.choice(range(1, len(list) + 1))
        random_sample = [list[i] for i in random.sample(xrange(len(list)), size_of_sample)]
        return random_sample

    def _random_constant(self, field=None):
        if field:
            if field == "int_field1":
                random_constant = random.randrange(36787, 99912344, 1000000)
            elif field == "bool_field1":
                random_constant = random.choice([True, False])
            elif field == "char_field1":
                random_constant = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(1))
                random_constant = "'%s'" %random_constant
            elif field == "datetime_field1":
                random_constant = "'%s'" % self._random_datetime()
            elif field == "decimal_field1":
                random_constant = random.randrange(16, 9971, 10)
            elif field == "primary_key_id":
                random_constant = "'%s'" % random.randrange(1, 9999, 10)
            elif field == "varchar_field1":
                random_constant = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(5))
                random_constant = "'%s'" %random_constant
            else:
                print("Unknown field type: " + str(field))
                exit(1)
        else:
            constant_type = random.choice(["STRING", "INTEGER", "DECIMAL", "BOOLEAN"])
            if constant_type == "STRING":
                random_constant = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
                random_constant = "'%s'" %random_constant
            elif constant_type == "INTEGER":
                random_constant = random.randrange(36787, 99912344, 1000000)
            elif constant_type == "DECIMAL":
                random_constant = float((random.randrange(700, 997400))/100)
            elif constant_type == "BOOLEAN":
                random_constant = random.choice([True, False])
            else:
                print("Unknown constant type")
                exit(1)
        return random_constant

    def _remove_trailing_substring(self, string, ending):
        if string.endswith(ending):
            return string[:-len(ending)]
        else:
            return string

    def _extract_fields_from_clause(self, clause_type, template_map, conversion_map, key_path=[]):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        table_fields = table_map[table_name]["fields"].keys()
        expression = self.get_from_dict(template_map, key_path+[clause_type + "_CLAUSE"])
        if expression.find(clause_type) == -1:
            return []
        if clause_type == "SELECT":
            expression_fields_string = expression.split("SELECT")[1].split("FROM")[0].strip()
        else:
            print("Unknown clause type")
            exit(1)

        return self._extract_raw_fields_from_string(table_fields, expression_fields_string)

    def _extract_raw_fields_from_string(self, raw_field_list, string):
        raw_fields = []
        for raw_field in raw_field_list:
            if raw_field.find('char') == 0:
                idx = self.find_char_field(string)
                if idx > -1:
                    raw_fields.append((str(raw_field), idx))
            else:
                idx = string.find(raw_field)
                if idx > -1:
                    raw_fields.append((str(raw_field), idx))
        return raw_fields

    def _convert_sql_template_for_skip_range_scan(self, n1ql_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        aggregate_pushdown = "secondary"
        sql, table_map = self._convert_sql_template_to_value(sql=n1ql_template, table_map=table_map, table_name=table_name, aggregate_pushdown=aggregate_pushdown, ansi_joins=False)
        n1ql = self._gen_sql_to_nql(sql, ansi_joins=False)
        sql = self._convert_condition_template_to_value_datetime(sql, table_map, sql_type="sql")
        n1ql = self._convert_condition_template_to_value_datetime(n1ql, table_map, sql_type="n1ql")
        sql_map = self._divide_sql(n1ql)

        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = {"n1ql": n1ql,
               "sql": sql,
               "bucket": str(",".join(table_map.keys())),
               "expected_result": None,
               "indexes": {}
               }

        table_name = random.choice(table_map.keys())
        map["bucket"] = table_name
        table_fields = table_map[table_name]["fields"].keys()

        aggregate_pushdown_index_name, create_aggregate_pushdown_index_statement = self._create_skip_range_key_scan_index(table_name, table_fields, sql_map)
        map = self.aggregate_special_convert(map)
        map["indexes"][aggregate_pushdown_index_name] = {"name": aggregate_pushdown_index_name,
                                                         "type": "GSI",
                                                         "definition": create_aggregate_pushdown_index_statement
                                                         }
        return map

    def _create_skip_range_key_scan_index(self, table_name, table_fields, sql_map):
        where_condition = sql_map["where_condition"]
        select_from = sql_map["select_from"]
        group_by = sql_map["group_by"]
        select_from_fields = []
        where_condition_fields = []
        groupby_fields = []
        aggregate_pushdown_fields_in_order = []
        skip_range_scan_index_fields_in_order = []

        for field in table_fields:
            if field.find('char') == 0:
                if select_from:
                    idx = self.find_char_field(select_from)
                    if idx > -1:
                        select_from_fields.append((idx, field))
                if where_condition:
                    idx = self.find_char_field(where_condition)
                    if idx > -1:
                        where_condition_fields.append((idx, field))
                if group_by:
                    idx = self.find_char_field(group_by)
                    if idx > -1:
                        groupby_fields.append((idx, field))
            else:
                if select_from:
                    idx = select_from.find(field)
                    if idx > -1:
                        select_from_fields.append((idx, field))
                if where_condition:
                    idx = where_condition.find(field)
                    if idx > -1:
                        where_condition_fields.append((idx, field))
                if group_by:
                    idx = group_by.find(field)
                    if idx > -1:
                        groupby_fields.append((idx, field))

        select_from_fields.sort(key=lambda tup: tup[0])
        where_condition_fields.sort(key=lambda tup: tup[0])
        groupby_fields.sort(key=lambda tup: tup[0])

        leading_key = random.choice(where_condition_fields)
        skip_range_scan_index_fields_in_order.append(leading_key[1])
        where_condition_fields.remove(leading_key)
        all_fields = select_from_fields + where_condition_fields + groupby_fields

        for i in range(0, len(all_fields)):
            num_random_fields = random.choice([1, 2, 3])
            for j in range(0, num_random_fields):
                random_field = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
                skip_range_scan_index_fields_in_order.append(random_field)
            next_field = random.choice(all_fields)
            all_fields.remove(next_field)
            skip_range_scan_index_fields_in_order.append(next_field[1])

        aggregate_pushdown_index_name = "{0}_aggregate_pushdown_index_{1}".format(table_name, self._random_int())

        create_aggregate_pushdown_index = \
                        "CREATE INDEX {0} ON {1}({2}) USING GSI".format(aggregate_pushdown_index_name, table_name, self._convert_list(skip_range_scan_index_fields_in_order, "numeric"))
        return aggregate_pushdown_index_name, create_aggregate_pushdown_index

    ''' Main function to convert templates into SQL and N1QL queries for GROUP BY clause field aliases '''
    def _convert_sql_template_for_group_by_aliases(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        n1ql_template_map = self._divide_sql(query_template)

        sql_template_map = copy.copy(n1ql_template_map)
        sql_template_map = self._add_field_aliases_to_sql_select_clause(sql_template_map)
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'where_condition')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'order_by')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'group_by')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'having')

        sql_table, table_map = self._gen_select_tables_info(sql_template_map["from_fields"], table_map, False)
        converted_sql_map, converted_n1ql_map = self._apply_group_by_aliases(sql_template_map, n1ql_template_map, table_map)
        n1ql, sql, table_map = self._convert_sql_n1ql_templates_to_queries(converted_n1ql_map=converted_n1ql_map,
                                                                                         converted_sql_map=converted_sql_map,
                                                                                         table_map=table_map,
                                                                                         table_name=table_name)
        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = { "n1ql": n1ql,
                "sql": sql,
                "bucket": str(",".join(table_map.keys())),
                "expected_result": None,
                "indexes": {} }
        return map

    ''' Function builds whole SQL and N1QL queries after applying field aliases to all available query clauses. '''
    def _convert_sql_n1ql_templates_to_queries(self, converted_n1ql_map={}, converted_sql_map={}, table_map={}, table_name="simple_table"):

        new_sql = "SELECT " + converted_sql_map['select_from'] + " FROM " + table_name
        if converted_sql_map['where_condition']:
            new_sql += " WHERE " + converted_sql_map['where_condition']
        if converted_sql_map['group_by']:
            new_sql += " GROUP BY " + converted_sql_map['group_by']
        if converted_sql_map['having']:
            new_sql += " HAVING " + converted_sql_map['having']
        if converted_sql_map['order_by']:
            new_sql += " ORDER BY " + converted_sql_map['order_by']

        new_n1ql = "SELECT " + converted_n1ql_map['select_from'] + " FROM " + table_name
        if converted_n1ql_map['where_condition']:
            new_n1ql += " WHERE " + converted_n1ql_map['where_condition']
        if converted_n1ql_map['group_by']:
            new_n1ql += " GROUP BY " + converted_n1ql_map['group_by']
        if converted_n1ql_map['having']:
            new_n1ql += " HAVING " + converted_n1ql_map['having']
        if converted_n1ql_map['order_by']:
            new_n1ql += " ORDER BY " + converted_n1ql_map['order_by']

        return new_n1ql, new_sql, table_map

    ''' Function constructs temp dictionaries for SQL and N1QL query clauses and passes them to 
        transform function. '''
    def _apply_group_by_aliases(self, sql_template_map={}, n1ql_template_map={}, table_map={}):

        string_field_names = self._search_fields_of_given_type(["varchar", "text", "tinytext", "char"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime"], table_map)
        bool_field_names = self._search_fields_of_given_type(["tinyint"], table_map)

        converted_sql_map = copy.deepcopy(sql_template_map)
        converted_n1ql_map = copy.deepcopy(n1ql_template_map)

        if "BOOL_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(bool_field_names,
                                                                                    "BOOL_ALIAS",
                                                                                    "BOOL_FIELD",
                                                                                    n1ql_template_map,
                                                                                    converted_n1ql_map,
                                                                                    converted_sql_map)
        if "STRING_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(string_field_names,
                                                                                 "STRING_ALIAS",
                                                                                 "STRING_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)
        if "NUMERIC_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(numeric_field_names,
                                                                                 "NUMERIC_ALIAS",
                                                                                 "NUMERIC_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)
        if "DATETIME_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(datetime_field_names,
                                                                                 "DATETIME_ALIAS",
                                                                                 "DATETIME_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)

        return converted_sql_map, converted_n1ql_map

    def _add_field_aliases_to_sql_select_clause(self, sql_map):
        select_from = sql_map['select_from']

        select_from = self._add_field_alias(select_from, "NUMERIC_FIELD", "NUMERIC_ALIAS")
        select_from = self._add_field_alias(select_from, "VARCHAR_FIELD", "VARCHAR_ALIAS")
        select_from = self._add_field_alias(select_from, "BOOL_FIELD", "BOOL_ALIAS")
        select_from = self._add_field_alias(select_from, "DATETIME_FIELD", "DATETIME_ALIAS")

        sql_map['select_from'] = select_from
        return sql_map

    def _add_field_alias(self, select_from, field_token, alias_token):
        if not (field_token + " " + alias_token) in select_from:
            if alias_token in select_from:
                select_from = select_from.replace(alias_token, field_token + " " + alias_token)
            else:
                select_from = select_from.replace(field_token, field_token + " " + alias_token)
        return select_from

    def _cleanup_field_aliases_from_sql_clause(self, sql_map={}, clause_name=''):
        if clause_name in sql_map.keys():
            clause_str = sql_map[clause_name]

            clause_str = clause_str.replace("NUMERIC_FIELD NUMERIC_ALIAS", "NUMERIC_FIELD").\
                replace("BOOL_FIELD BOOL_ALIAS", "BOOL_FIELD").replace("STRING_FIELD STRING_ALIAS", "STRING_FIELD").\
                replace("DATETIME_FIELD DATETIME_ALIAS", "DATETIME_FIELD")

            clause_str = clause_str.replace("NUMERIC_ALIAS", "NUMERIC_FIELD").replace("BOOL_ALIAS", "BOOL_FIELD").\
                replace("STRING_ALIAS", "STRING_FIELD").replace("DATETIME_ALIAS", "DATETIME_FIELD")

            sql_map[clause_name] = clause_str

        return sql_map


    ''' Function substitutes XXX_FIELD and XXX_ALIAS placeholders with real field names and their aliases.
        Additional transformations to stay compatible with common SQL and N1QL syntax.
        Additional transformations to produce the same fields aliases usage for SELECT clause in SQL and N1QL queries.'''
    def normalize_field_aliases(self, field_names, alias_token, field_token, n1ql_map, converted_n1ql_map, converted_sql_map):
        field_name = random.choice(field_names)

        group_by_alias = alias_token in n1ql_map["group_by"]
        select_alias = (field_token+" "+alias_token) in n1ql_map["select_from"]

        if alias_token in n1ql_map["group_by"]:
            converted_n1ql_map['group_by'] = converted_n1ql_map['group_by'].replace(alias_token, field_name[:-1])

        if (field_token+" "+alias_token) in n1ql_map["select_from"]:
            converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
            converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
        elif field_token in n1ql_map["select_from"]:
            converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token, field_name)
            converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, field_name)
        else: #BOOL_ALIAS
            if group_by_alias:
                converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(alias_token, field_name[:-1])
                converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
            else:
                converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(alias_token, field_name)
                converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, field_name)

        if alias_token in n1ql_map['order_by']:
            if group_by_alias or select_alias:
                converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(alias_token, field_name[:-1])
            else:
                converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(alias_token, field_name)

        if alias_token in n1ql_map['having']:
            if group_by_alias:
                converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(alias_token, field_name[:-1])
            else:
                converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(alias_token, field_name)

        converted_n1ql_map['group_by'] = converted_n1ql_map['group_by'].replace(field_token, field_name)
        converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token, field_name)
        converted_n1ql_map['where_condition'] = converted_n1ql_map['where_condition'].replace(field_token, field_name)
        converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(field_token, field_name)
        converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(field_token, field_name)

        converted_sql_map['group_by'] = converted_sql_map['group_by'].replace(field_token, field_name)
        converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token, field_name)
        converted_sql_map['where_condition'] = converted_sql_map['where_condition'].replace(field_token, field_name)
        converted_sql_map['order_by'] = converted_sql_map['order_by'].replace(field_token, field_name)
        converted_sql_map['having'] = converted_sql_map['having'].replace(field_token, field_name)

        converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(alias_token, field_name[:-1])

        return converted_n1ql_map, converted_sql_map

