import sqlite3 
import requests 
import unittest 

from  parser import *


class IndexerTest(unittest.TestCase):
    def test_index(self):
        indexer = Indexer(4, 50, 20)

        self.assertEqual(indexer.index(4, 20), 1)
        self.assertEqual(indexer.index(4, 1), 20)
        self.assertEqual(indexer.index(3, 40), 31)
        self.assertEqual(indexer.index(3, 1), 70)

    def test_rev_index_2(self):
        indexer = Indexer(2, 5, 5)

        self.assertEqual(indexer.rev_index(10), (1, 1))

    def test_rev_index(self):
        indexer = Indexer(4, 50, 20)

        self.assertEqual(indexer.rev_index(1), (4, 20))
        self.assertEqual(indexer.rev_index(20), (4, 1))
        self.assertEqual(indexer.rev_index(31), (3, 40))
        self.assertEqual(indexer.rev_index(70), (3, 1))

    def test_total_range(self):
        indexer = Indexer(4, 50, 20)

        self.assertEqual(indexer.total_range(), (1, 170))

    def test1(self):
        indexer = Indexer(4, 50, 20)

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

class RangeHolesTest(unittest.TestCase):
    def test_range_holes_1(self):
        ranges = [(1, 7), (10, 15)]
        min_max_range = (1, 20)
        holes = [(8, 9), (16, 20)]

        self.assertEqual(find_range_holes(ranges, min_max_range), holes)

    def test_range_holes_2(self):
        ranges = [(2, 7), (10, 15)]
        min_max_range = (1, 20)
        holes = [(1, 1), (8, 9), (16, 20)]

        self.assertEqual(find_range_holes(ranges, min_max_range), holes)

    def test_range_holes_3(self):
        ranges = [(2, 7), (10, 19)]
        min_max_range = (1, 20)
        holes = [(1, 1), (8, 9), (20, 20)]

        self.assertEqual(find_range_holes(ranges, min_max_range), holes)

    def test_range_holes_4(self):
        ranges = [(2, 7), (10, 20)]
        min_max_range = (1, 20)
        holes = [(1, 1), (8, 9)]

        self.assertEqual(find_range_holes(ranges, min_max_range), holes)

    def test_reflexivity(self):
        ranges = [(2, 7), (10, 20)]
        min_max_range = (1, 20)
        self.assertEqual(find_range_holes(find_range_holes(ranges, min_max_range), min_max_range), ranges)

        ranges = [(2, 7), (10, 15)]
        min_max_range = (1, 20)
        self.assertEqual(find_range_holes(find_range_holes(ranges, min_max_range), min_max_range), ranges)

class RangeDownloaderTest(unittest.TestCase):
    def test_correctness(self):
        # (total_page, last_page_items) = get_page_info() 
        # print("#total pages {}, #items in last page {}".format(total_page, last_page_items))
        # indexer = Indexer(total_page, 5, last_page_items)
        r = (1, 10)
        # pool = Pool()
        # records = download_range(r, indexer, pool)

        # self.assertEqual(len(records), 10) 

class MajorityVoteTest(unittest.TestCase):
  def test_1(self):
    data = [2, 2, 5, 6, 7]
    self.assertEqual(majority_vote(data), 2)

if __name__ == "__main__":
    unittest.main()
