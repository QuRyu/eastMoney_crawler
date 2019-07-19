import json 
import time 
import datetime 
import logging 
from itertools import groupby 
from collections import namedtuple 
from multiprocessing import Pool 

import requests 
import sqlite3 

def generate_url(page_number: int) -> str:
    return "http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page={}&js=var%20AoofQLPM&param=&sortRule=-1&sortType=0&rt=52045947".format(page_number)

# check if the table with the name has been created 
def checkTableExistence(conn, name): 
    c = conn.cursor() 
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)) 
    return len(c.fetchall()) == 1 

def getPageContent(url):
    request = requests.get(url=url) 
    try:
        data = json.loads(request.text[13:])
    except JSONDecodeError as e:
        logging.warn("JSON decode error on url {}\ntry again".format(url))
        time.sleep(2)
        return getPageContent(url)

    if data['success'] and len(data['data']) != 0:
        return data
    else: 
        time.sleep(2)
        getPageContent(url)

# merge ranges together 
# e.g. merge [(1,3), (4,7), (10, 12), (13, 15)] into [(1, 7), (10, 15)]
# Invariance: 1. Each range does not overlap with others, 
#                 e.g. given two ranges (a, b) and (c, d), it must follow either a < b < c < d or 
#                       c < d < a < b 
#             2. Input is sorted by the start key 
def merge_ranges(history):
    length = len(history)
    if length < 2:
        return history
    elif length == 2:  # merge them if possible 
        if history[0][1] + 1 == history[1][0]:
            return [(history[0][0], history[1][1])]
        else:
            return history
    else:
        middle = length / 2 
        left = merge_ranges(history[:middle])
        right = merge_ranges(history[middle:])

        (llen, rlen) = (len(left), len(right))

        if left[llen-1][1] + 1 == right[0][0]:
            merged = (left[llen-1][0], right[0][1])
            return llen[:llen-1].append(merged).extend(right[1:])
        else:
            left.extend(right)

def find_range_holes(ranges, min_max_range):
    if len(ranges) == 0:
        return min_max_range

    (min, max) = min_max_range
    holes = [] 

    # find the hole between min and ranges 
    if min != ranges[0][0]: 
        holes.append((min, ranges[0][0]-1))

    # find holes between ranges 
    for i in range(0, len(ranges)-1):
        if ranges[i][1] + 1 != ranges[i+1][0]:
            holes.append((ranges[i][1]+1, ranges[i+1][0]-1))

    # hole between max and ranges 
    ranges_len = len(ranges)
    if max != ranges[ranges_len-1][1]:
        holes.append((ranges[ranges_len-1][1]+1, max))

    return holes


class Indexer:
    def __init__(self, total_page, items_in_page, last_page_items):
        self.total_page = total_page
        self.last_page_items = last_page_items
        self.items_in_page = items_in_page

        self.total_items = (total_page-1) * self.items_in_page + last_page_items

    def index(self, page: int, pos: int) -> int:
        if page == self.total_page:
            return self.last_page_items - pos + 1
        else:
            return (self.total_page - page - 1) * self.items_in_page + self.last_page_items + (self.items_in_page - pos + 1)

    def rev_index(self, id: int) -> (int, int): 
        if id <= self.last_page_items:
            return (self.total_page, self.last_page_items-id+1)
        else:
            id_cleaned = id - self.last_page_items
            relative_pos = id_cleaned % self.items_in_page 

            relative_page = int(id_cleaned / self.items_in_page)
            if id_cleaned == relative_page * self.items_in_page: 
                relative_page -= 1 
                relative_pos = self.items_in_page 

            return (self.total_page-1-relative_page, self.items_in_page-relative_pos+1)

    def total_range(self) -> (int, int): 
        return (1, (self.total_page-1)*self.items_in_page + self.last_page_items)

def pull_history(conn, indexer: Indexer) -> [(int, int)]:
    c = conn.cursor()
    if not checkTableExistence("history"): # create the table 
        c.execute('''CREATE TABLE history
                     (index_start INTEGER, index_end INTEGER)''')
        conn.commit()
        return []
    else:
        c.execute("SELECT * FROM history ORDER BY index_start")
        history = cursor.fetch_all()
        if len(history) == None:
            return []
        else:
            ranges = merge_ranges(history)
            return ranges

def majority_vote(xs): 
    count = {} 

    for x in xs:
        if x not in count:
            count[x] = 1
        else:
            count[x] = count[x] + 1 

    largest = -1 
    key = -1
    for (k, v) in count.items():
        if v > largest:
            key = k 
            largest = v 

    return key 

# returns (number of pages, #items in last page) 
def get_page_info() -> (int, int):
    def total_page_number(): 
        URL = generate_url(1)
        data = getPageContent(URL)
        return data['pages']

            
    numbers = [] 
    for _ in range(3): 
        numbers.append(total_page_number())
        time.sleep(5)
    N = majority_vote(numbers)

    URL = generate_url(N)
    data = getPageContent(URL)['data']

    return (N, len(data))

def check_record_table(conn):
    if not checkTableExistence(conn, 'records'):
        c = conn.cursor()
        c.execute(''' CREATE TABLE records 
                      (CompanyCode text, CompanyName text, OrgCode text, 
                      OrgName text, OrgSum text, SCode text, SName text,
                      NoticeDate text, SurveyDate text, EndDate text, 
                      Place text, Description text, Orgtype text, 
                      OrgtypeName text, Personnel text, Licostaff text,
                      Maincontent text) ''')


KEYS_DELETE = ['ChangePercent', 'Close']
def process(t):
    (page, items_loc) = t
    content = getPageContent(generate_url(page))
    data = content['data']

    items_loc.sort()
    records = [] 
    for i in items_loc:
        record = data[i-1] 
        for key in KEYS_DELETE:
          del record[key] 
        records.append(record)

    return records 

def download_range(r: (int, int), indexer: Indexer, pool: Pool):
    (left, right) = r 
    page_loc = [] 
    for i in range(left, right+1):
        page_loc.append(indexer.rev_index(i))
        
    item_loc = [] 
    for k, g in groupby(page_loc, lambda x: x[0]):
        mapeed_g = [el[1] for el in map(tuple, g)]
        item_loc.append((k, mapeed_g))

    records = pool.map(process, item_loc)

    flatten = lambda l: [item for sublist in l for item in sublist]
    records = flatten(records)

    return records

def break_range_if_too_large(r):
  (start, end) = r 

  if end - start > 500:
    intervals = [] 

    while start + 500 < end:
      intervals.append((start, start + 500))
      start += 500 

    if start < end:
      intervals.append((start+1, end))

    return intervals

  else:
    return [r] 


def main():
  def v_gen(xs):
    for x in xs:
      yield list(x.values())
  conn = sqlite3.connect("survey.db") 

  (total_page, last_page_items) = get_page_info()
  indexer = Indexer(total_page, last_page_items, 50)

  pool = Pool()

  check_record_table()
  history = pull_history(conn, indexer)
  holes = find_range_holes(history)
  
  for interval in holes:
    for subinterval in break_range_if_too_large(interval):
      (start, end) = subinterval
      logging.info("downloading records of range ({}, {})".format(start, end))

      records = download_range(subinterval, indexer, pool)
      c = conn.cursor()
      c.execute("BEGIN TRANSACTION;")
      c.executemany('''INSERT INTO records VALUES 
                            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''',  
                            v_gen(records))
      c.execute("INSERT INTO history VALUES (?,?);", [start, end])
      c.execute("COMMIT;")


if __name__ == "__main__":
    main()



