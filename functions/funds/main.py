import random
import json
import requests
from pymongo import MongoClient
from fake_headers import Headers
from datetime import datetime


client_mdb = MongoClient("mongodb://gc_f_user:9SjYdJbCuMm1qg5L@cluster0-shard-00-00.mhkp0.mongodb.net:27017,cluster0-shard-00-01.mhkp0.mongodb.net:27017,cluster0-shard-00-02.mhkp0.mongodb.net:27017/<dbname>?ssl=true&replicaSet=atlas-enfcuj-shard-0&authSource=admin&retryWrites=true&w=majority")
db = client_mdb.data


def hello_world(request):
    request_json = request.get_json()
    if request_json and 'message' in request_json:
        c_l = request_json['message']
        url = 'https://www.crunchbase.com/v4/data/entities/funds/{}?field_ids=%5B%22identifier%22,%22layout_id%22,%22facet_ids%22,%22title%22,%22short_description%22,%22is_locked%22%5D&layout_mode=view'.format(
            c_l)
        headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        }
        headers = Headers(os="linux", headers=True).generate()
        r = requests.get(url=url, headers=headers)
        if r.status_code == 200:
            r_json = json.loads(r.content)
            print("successful...")
            db.funds_and_fr_simple_entity.insert_one(
                {"funds": c_l, "data": r_json, 'insert_date': datetime.utcnow()})

            return f'worked'
        else:
            print(f"Failed to fetch: '{c_l}': ")
            print("---------\n", r.text)
            return f'Didnt work'
    else:
        return f'Hello World!'
