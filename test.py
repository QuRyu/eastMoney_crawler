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

    def test_merge_ranges_2(self):
        ranges = [(1, 501), (502, 1002), (1003, 1503), (1504, 2004), (2005, 2505), (2506, 3006), (3007, 3507), (3508, 4008), (4009, 4509), (4510, 5010), (5011, 5511), (5512, 6012), (6013, 6513), (6514, 7014), (7015, 7515), (7516, 8016), (8017, 8517), (8518, 9018), (9019, 9519), (9520, 10020), (10021, 10521), (10522, 11022), (11023, 11523), (11524, 12024), (12025, 12525), (12526, 13026), (13027, 13527), (13528, 14028), (14029, 14529), (14530, 15030), (15031, 15531), (15532, 16032), (16033, 16533), (16534, 17034), (17035, 17535), (17536, 18036), (18037, 18537), (18538, 19038), (19039, 19539), (19540, 20040), (20041, 20541), (20542, 21042), (21043, 21543), (21544, 22044), (22045, 22545), (22546, 23046), (23047, 23547), (23548, 24048), (24049, 24549), (24550, 25050), (25051, 25551)]
        merged_ranges = [(1, 25551)]

        self.assertEqual(len(merge_ranges(ranges)), 1)
        self.assertEqual(merge_ranges(ranges), merged_ranges)


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

    def test_range_holes_5(self):
        indexer = Indexer(6912, 50, 10) 
        history = [(1, 501), (502, 1002), (1003, 1503), (1504, 2004), (2005, 2505), (2506, 3006), (3007, 3507), (3508, 4008), (4009, 4509), (4510, 5010), (5011, 5511), (5512, 6012), (6013, 6513), (6514, 7014), (7015, 7515), (7516, 8016), (8017, 8517), (8518, 9018), (9019, 9519), (9520, 10020), (10021, 10521), (10522, 11022), (11023, 11523), (11524, 12024), (12025, 12525), (12526, 13026), (13027, 13527), (13528, 14028), (14029, 14529), (14530, 15030), (15031, 15531), (15532, 16032), (16033, 16533), (16534, 17034), (17035, 17535), (17536, 18036), (18037, 18537), (18538, 19038), (19039, 19539), (19540, 20040), (20041, 20541), (20542, 21042), (21043, 21543), (21544, 22044), (22045, 22545), (22546, 23046), (23047, 23547), (23548, 24048), (24049, 24549), (24550, 25050), (25051, 25551)] 
        history = merge_ranges(history)
        holes = find_range_holes(history, indexer.total_range())

        self.assertEqual(len(history), 1)
        self.assertEqual(holes, [(25552, indexer.total_range()[1])])



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

class HelperMethods(unittest.TestCase):
  def test_majority_vote(self):
    data = [2, 2, 5, 6, 7]
    self.assertEqual(majority_vote(data), 2)

  def test_break_range_if_too_large(self):
    r = (2, 3)
    self.assertEqual(break_range_if_too_large(r), [r])

    r = (1, 502)  
    self.assertEqual(break_range_if_too_large(r), [(1, 501), (502, 502)])

    r = (1, 800)
    data = [(1, 501), (502, 800)]
    self.assertEqual(break_range_if_too_large(r), data)

    r = (1, 2000)
    data = [(1, 501), (502, 1002), (1003, 1503), (1504, 2000)]
    self.assertEqual(break_range_if_too_large(r), data)
  


if __name__ == "__main__":
    unittest.main()
