import unittest
import os
import pyorient
from pyorient.messages.base import BaseMessage
from pyorient.constants import FIELD_INT, FIELD_BOOLEAN, FIELD_STRING
from pyorient.exceptions import PyOrientCommandException
import tests

os.environ['DEBUG'] = "1"
os.environ['DEBUG_VERBOSE'] = "0"

class OrientMessageTestCase(unittest.TestCase):

    def test_exception_message_without_token(self):
        client = pyorient.OrientDB("localhost", 2424)
        session_id = client.connect("root", "root")
        db_name = "dummy_db"
        ex_msg = ''
        try:
            client.db_drop(db_name)
        except PyOrientCommandException as e:
            ex_msg = f'{e}'
        finally:
            print(ex_msg)
        assert len(ex_msg) > 0

    def test_exception_message_with_token(self):
        factory = pyorient.OrientDB('localhost', 2424)
        factory.get_message( pyorient.CONNECT ).prepare( ("root", "root") ).send().fetch_response()
        db_name = 'dummy_db'
        ex_msg = ''
        try:
            ( factory.get_message( pyorient.DB_DROP ) ).prepare([db_name]).send().fetch_response()
        except PyOrientCommandException as e:
            ex_msg = f'{e}'
        finally:
            print(ex_msg)
        assert len(ex_msg) > 0
