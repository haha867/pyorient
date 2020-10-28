#!/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Ostico <ostico@gmail.com>'
import unittest
import os

os.environ['DEBUG'] = "0"
os.environ['DEBUG_VERBOSE'] = "0"

import pyorient


class CommandTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CommandTestCase, self).__init__(*args, **kwargs)
        self.client = None

    def setUp(self):

        self.client = pyorient.OrientDB("localhost", 2424)
        self.client.connect("root", "root")

        db_name = "test_tr"
        try:
            self.client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = self.client.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                                       pyorient.STORAGE_TYPE_MEMORY)

        self.cluster_info = self.client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )


    def collect_class_data(self):
        cls_data = self.client.command(
            "select expand(classes) from metadata:schema"
        )
        return { k.oRecordData['name']:k.oRecordData for k in cls_data }

    def test_reserved_words(self):

        class_id1 = self.client.command("create class my_v_class extends V")[0]
        class_id1 = self.collect_class_data()['my_v_class']['defaultClusterId']
        class_id2 = self.client.command("create class str extends E")[0]
        class_id2 = self.collect_class_data()['str']['defaultClusterId']
        rec1 = {'@my_v_class': {'accommodation': 'house', 'work': 'office',
                                'holiday': 'sea'}}
        rec2 = {'@my_v_class': {'accommodation': 'house', 'work2': 'office',
                                'holiday': 'sea3'}}
        rec_position1 = self.client.record_create(class_id1, rec1)
        rec_position2 = self.client.record_create(class_id1, rec2)

        sql_edge = "create edge from " + rec_position1._rid + " to " + \
                   rec_position2._rid
        res = self.client.command(sql_edge)

        # print (res[0]._in)
        assert isinstance(res[0]._in,
                          pyorient.OrientRecordLink)
        assert res[0]._in.get_hash() == rec_position2._rid

        # print (res[0]._out)
        assert isinstance(res[0]._out, pyorient.OrientRecordLink)
        assert res[0]._out.get_hash() == rec_position1._rid

        result = self.client.query(
            "select @rid, @version, holiday from V")
        # for x in result:
        # print ( "%r" % x._rid.get() )
        # print ( "%r" % x._rid.get_hash() )
        # print ( "%r" % x.holiday )
        # print ( "%r" % x._version )

        assert result[0].oRecordData['rid'].get() == f'{class_id1}:0' #11:0'
        assert result[0].rid.get_hash() == rec_position1._rid
        assert result[0].holiday == rec1['@my_v_class']['holiday']
        assert result[0].version != 0

        assert result[1].rid.get() == f'{class_id1}:1' #11:1'
        assert result[1].rid.get_hash() == rec_position2._rid
        assert result[1].holiday == rec2['@my_v_class']['holiday']
        assert result[0].version != 0

        x = self.client.command(
            "insert into V ( rid, version, model, ciao )" +
            " values ('test_rid', 'V1', '1123', 1234)")

        assert x[0].ciao == 1234

        x = self.client.command("select rid, @rid, model, ciao from V")
        import re
        assert re.match( '#[-]*[0-9]+:[0-9]+', x[0]._rid ), (
            "Failed to assert that "
            "'#[-]*[0-9]+:[0-9]+' matches received "
            "value: '%s'" % x[0]._rid
        )
        print( x[0]._rid )

        assert x[0].rid == 'test_rid'
        try:
            x[0]._rid.get_hash()
            assert False
        except AttributeError:
            assert True

#        assert x[0].rid2.get_hash() == '#9:0', ("Failed to assert that "
#                                                "'#9:0' equals received "
#                                                "value: '%s'" % x[0]._rid2)
        hash_id = f"#{self.collect_class_data()['V']['defaultClusterId']}:0"
        assert x[0].rid2.get_hash() == hash_id, ("Failed to assert that "
                                                 f"{hash_id}' equals received "
                                                 f"value: '{x[0].rid2}'")
        assert x[0].model == '1123'
        assert x[0].ciao == 1234

    def test_new_projection(self):
        rec = {'@Package': {'name': 'foo', 'version': '1.0.0', 'rid': 'this_is_fake'}}
        v_cluster = self.collect_class_data()['V']['defaultClusterId']
        x = self.client.record_create(v_cluster, rec)
        assert x._rid == f'#{v_cluster}:0' #'#9:0'
        import re
        # this can differ from orientDB versions, so i use a regular expression
        assert re.match( '[0-1]', str( x._version ) )
        assert x._class == 'Package'
        assert x.name == 'foo'
        assert x.version == '1.0.0'
        assert x.rid == 'this_is_fake'
        assert x.oRecordData['name'] == 'foo'
        assert x.oRecordData['version'] == '1.0.0'
        assert x.oRecordData['rid'] == 'this_is_fake'

    def test_sql_batch(self):
        cmd = "begin;" + \
              "let a = create vertex set script = true;" + \
              "let b = select from v limit 1;" + \
              "let e = create edge from $a to $b;" + \
              "commit retry 100;"

        edge_result = self.client.batch(cmd)

        # print( cluster_id[0] )
        # print (cluster_id[0]._in)
        v_cluster = self.collect_class_data()['V']['defaultClusterId']
        rid0 = f'#{v_cluster}:0'
        assert isinstance(edge_result[0]._in,
                          pyorient.OrientRecordLink)
#        assert edge_result[0]._in.get_hash() == "#9:0", \
#            "in is not equal to '#9:0': %r" % edge_result[0]._in.get_hash()
        assert edge_result[0]._in.get_hash() == rid0, \
            f"in is not equal to {rid0} : {edge_result[0]._in.get_hash()}"

        # print (cluster_id[0]._out)
        assert isinstance(edge_result[0]._out, pyorient.OrientRecordLink)

    def test_sql_batch_2(self):

        cluster_id = self.client.command("create class fb extends V")
        cluster_id = self.client.command("create class response extends V")
        cluster_id = self.client.command("create class followed_by extends E")

        cluster_id = self.client.batch( (
            "begin;"
            "let a = create vertex fb set name = 'd1';"
            "let b = create vertex response set name = 'a1';"
            "create edge followed_by from $a to $b;"
            "commit;"
        ) )

    def test_sql_batch_3(self):

        cluster_id = self.client.command("create class fb extends V")
        cluster_id = self.client.command("create class response extends V")
        cluster_id = self.client.command("create class followed_by extends E")

        cmd = (
            "BEGIN; "
            "let a = create vertex fb set name = 'a1'; "
            "let d = create vertex response set name = 'd1'; "
            "let e = create edge from $a to $d set name = 'fb to response'; "
            "COMMIT retry 100; "
            "return $e; "
        )

        # assert isinstance(self.cluster_info, pyorient.Information)

        # The preceding batch script create an exception
        # in OrientDB newest than 2.1
        if self.client.version.major == 2 and \
                self.client.version.minor >= 1:
            with self.assertRaises( pyorient.PyOrientCommandException ):
                cluster_id = self.client.batch(cmd)
        else:
            cluster_id = self.client.batch(cmd)

        print(f"created edge rid:{cluster_id[0]._rid}, name:{cluster_id[0].oRecordData['name']}!")



class CommandPrepareCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CommandPrepareCase, self).__init__(*args, **kwargs)
        self.client = None

    def setUp(self):

        self.client = pyorient.OrientDB("localhost", 2424)
        self.client.connect("root", "root")

        db_name = "GratefulDeadConcerts_test"
        try:
            self.client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = self.client.db_create(db_name, pyorient.DB_TYPE_GRAPH)

        self.cluster_info = self.client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

    def test_execute_sql_batch(self):

        cluster_id = self.client.command("create class person extends V")
        cluster_id = self.client.command("create class followed_by extends E")

        cmd = (
            "BEGIN; "
            "let a = create vertex person set role = 'pa'; "
            "let b = create vertex person set name = 'son'; "
            "let c = create edge followed_by from $a to $b set memo = 'pa to son'; "
            "let d = create edge followed_by from $b to $a set memo = 'son to pa'; "
            "COMMIT retry 100; "
        )

        # assert isinstance(self.cluster_info, pyorient.Information)

        # The preceding batch script create an exception
        # in OrientDB newest than 2.1
        if self.client.version.major == 2 and \
                self.client.version.minor >= 1:
            with self.assertRaises( pyorient.PyOrientCommandException ):
                cluster_id = self.client.batch(cmd)
        else:
            cluster_id = self.client.batch(cmd)

