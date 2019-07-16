import json 
import datetime 
from collections import namedtuple 
from multiprocessing import Pool 

import requests 
import sqlite3 


# check if the table with the name has been created 
def checkTableExistence(conn, name): 
    c = conn.cursor() 
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)) 
    return len(c.fetchall()) == 1 

def getPageContent(url):
    request = requests.get(url=url) 
    data = json.loads(request.text[13:])

    if data['success']:
        return data
    else: 
        getPageContent(url)


KEYS_DELETE = ['ChangePercent', 'Close']
def parse(URL): 
    data = getPageContent(URL)

    records = []
    for record in data['data']: 
        for key in KEYS_DELETE:
            del record[key] 
        records.append(record)

    return records 

class Indexer:
    def __init__(self, total_page, last_page_items):
        self.total_page = total_page
        self.last_page_items = last_page_items

        self.total_items = (total_page-1) * 50 + last_page_items

    def index(self, page: int, pos: int) -> int:
        return (self.total_page - page - 1) * 50 + self.last_page_items + (50 - pos + 1)

    def rev_index(self, id: int) -> (int, int): 
        if id <= self.last_page_items:
            return (self.total_page, self.last_page_items-id+1)
        else:
            id_cleaned = id - self.last_page_items
            relative_pos = id_cleaned % 50 

            relative_page = int(id_cleaned / 50)
            if id_cleaned == relative_page * 50: 
                relative_page -= 1 
                relative_pos = 50 

            return (self.total_page-1-relative_page, 50-relative_pos+1)


class UrlGenerator: 
    def __init__(self, conn):
        self._conn = conn 
        latest_page = self._get_latest_page() 
        total_pages = self._total_page_number()

        self._idx = 1
        if latest_page == None: # read in all pages 
            self._end = total_pages
        else: # only download updated part 
            diff = total_pages-latest_page
            self._end = 1 if diff == 0 else diff 

    # yield the next url for pulling data 
    # return value None indicates that the list of urls to pull data from is exhausted 
    def yield_url(self):
        if self._idx <= self._end: 
            url = "http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page={}&js=var%20AoofQLPM&param=&sortRule=-1&sortType=0&rt=52045947".format(self._idx)
            print("parsing page ", self._idx)
            self._idx += 1 
            return url 
        else: 
            return None 

    def update_db(self):
        date = datetime.datetime.now()
        cursor = self._conn.cursor()
        cursor.execute("INSERT INTO pullHistory VALUES (?,?)", [str(date.date()), self._total_pages])
        self._conn.commit()
 
    def _get_latest_page(self):
        if not checkTableExistence(self._conn, "pullHistory"):
            self._create_table()
            return None 
        
        # check if there is any query before 
        cursor = self._conn.cursor()
        cursor.execute("SELECT * FROM pullHistory ORDER BY pageNumber")
        history = cursor.fetchall()

        return history[-1][1]
    
    def _total_page_number(self): 
        URL = "http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page=1&js=var%20AoofQLPM&param=&sortRule=-1&sortType=0&rt=52045947"
        data = getPageContent(URL)
        self._total_pages = data['pages']

        return self._total_pages

    def _create_table(self):
        c = self._conn.cursor() 
        c.execute('''CREATE TABLE pullHistory
                     (date text, pageNumber, INTEGER)''')
        self._conn.commit()


class Scheduler:
    def __init__(self, links, conn, pool):
        self._links = links
        self._conn = conn 
        self._pool = pool

        # check if the table exists 
        if not checkTableExistence(self._conn, 'records'):
            c = self._conn.cursor()
            c.execute(''' CREATE TABLE records 
                          (CompanyCode text, CompanyName text, OrgCode text, 
                          OrgName text, OrgSum text, SCode text, SName text,
                          NoticeDate text, SurveyDate text, EndDate text, 
                          Place text, Description text, Orgtype text, 
                          OrgtypeName text, Personnel text, Licostaff text,
                          Maincontent text) ''')

    def run(self):
        def v_generator(records):
            for r in records:
                yield list(r.values())


        cursor = self._conn.cursor()
        run = True 

        while run: 
            urls = [] 
            for i in range(0, 10): 
                url = self._links.yield_url()
                if url != None:  # no more data to download 
                    urls.append(url)
                else:
                    run = False 
                    break 
            
            parsed = self._pool.map(parse, urls)
            records = [] 
            for record in parsed:
                records.extend(record)

            cursor.execute("BEGIN TRANSACTION;")
            cursor.executemany('''INSERT INTO records VALUES 
                                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',  
                                    v_generator(records))
            cursor.execute("COMMIT;")
            
    

def main():
    conn = sqlite3.connect("survey.db") 
    link = Links(conn)
    pool = Pool()
    scheduler = Scheduler(link, conn, pool) 

    # download new data 
    scheduler.run() 

    # cleanup 
    link.update_db()
    conn.commit() 
    conn.close()




if __name__ == "__main__":
    main()



