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

    def test_total_range(self):
        indexer = Indexer(4, 20)

        self.assertEqual(indexer.total_range(), (1, 170))

    def test1(self):
        indexer = Indexer(4, 20)

        index = 170
        for i in range(1, 4):
          for j in range(1, 51):
            self.assertEqual(indexer.index(i, j), index, "page pos ({}, {})".format(i, j))
            index -= 1 


class MergeRangeTest(unittest.TestCase):
    def merge_ranges_1(self): 
        ranges = [(1, 3), (4, 7), (10, 12), (13, 15)]
        merged_ranges = [(1, 7), (10, 15)]
        self.assertEqual(merge_ranges(ranges), merge_ranges)


if __name__ == "__main__":
    unittest.main()
