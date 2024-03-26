import unittest
from test import test_engine


def test():
    runner = unittest.TextTestRunner()
    runner.run(test_engine.suite())

if __name__ == "__main__":
    test()