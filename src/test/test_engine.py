import unittest
import sys
import os

sys.path.append("..")
from src.engine import utilities


class TestUtilities(unittest.TestCase):
    """Tests the engine.utilities module"""
    def setUp(self):
        self.path = os.getcwd()+"\\"
        self.filename = "test.db"
    
    def tearDown(self):
        try:
            os.remove(self.filename)
        except OSError:
            pass
    
    def test_create_schema(self):
        # Create test file
        v = utilities.create_schema(self.path, self.filename)
        # Test that file exists
        self.assertTrue(os.path.exists(self.path+self.filename))
        self.assertEqual(v, 0)
        # Create test file that already exists
        v = utilities.create_schema(self.path, self.filename)
        self.assertEqual(v, 1)
    
    def test_drop_schema(self):
        # Create test file
        open(self.filename, "a").close()
        # Test deleting a file
        v = utilities.drop_schema(self.path, self.filename)
        self.assertFalse(os.path.exists(self.path+self.filename))
        self.assertEqual(v, 0)
        # Test deleting a non-existing file
        v = utilities.drop_schema(self.path, self.filename)
        self.assertEqual(v, 1)
            
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestUtilities("test_create_schema"))
    suite.addTest(TestUtilities("test_drop_schema"))
    return suite