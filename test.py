import sqlite3 
import requests 
import unittest 

from  parser import *


class IndexerTest(unittest.TestCase):
    def test_index(self):
        indexer = Indexer(4, 20)

        self.assertEqual(indexer.index(4, 20), 1)
        self.assertEqual(indexer.index(4, 1), 20)
        self.assertEqual(indexer.index(3, 40), 31)
        self.assertEqual(indexer.index(3, 1), 70)

    def test_rev_index(self):
        indexer = Indexer(4, 20)

        self.assertEqual(indexer.rev_index(1), (4, 20))
        self.assertEqual(indexer.rev_index(20), (4, 1))
        self.assertEqual(indexer.rev_index(31), (3, 40))
        self.assertEqual(indexer.rev_index(70), (3, 1))


if __name__ == "__main__":
    unittest.main()
