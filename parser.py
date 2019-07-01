import json 
from collections import namedtuple 

import requests 

def pageParser(URL: str): 
    request = requests.get(url=URL) 
    data = json.loads(request.text[13:])

    records = []
    for record in data['data']: 
        records.append(record)

    return records 


