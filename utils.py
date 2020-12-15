from datetime import datetime, timedelta, timezone
import json
import os
from pymongo import MongoClient
import requests

MONGO_URL = 'mongodb://127.0.0.1:27017'
API_KEY = 'e727b291cb5877b8b5f3720506e7bc77'
DATABASE_NAME = 'CB_SCR_DB'
BASE_API_URL = 'https://api.crunchbase.com/api/v4/%s'


MONGO_URL = os.environ.get('MONGO_URL', MONGO_URL)
API_KEY = os.environ.get('CB_API_KEY', API_KEY)
DATABASE_NAME = os.environ.get('MONGO_DB_NAME', DATABASE_NAME)


def fetch_data(query, endpoint):
    """
        returns the result of a query of an endpoint.
    """
    
    # url = BASE_API_URL %(endpoint, API_KEY)
    url =  BASE_API_URL % endpoint
    apikey = {'user_key': API_KEY}
    print("The url is: ", url)
    req = requests.post(url, params=apikey, json=query)
    # print("Request: ", req.text)
    try:
        results = json.loads(req.text)
        total_count = results['count']
        entities = results['entities']
    except Exception as e:
        print("Error in parsing the API response: ", req.text)
        print(e)
        return None, None

    if len(entities) == 0:
        print("[Request function] entities is empty: ", req.text, ". The json formated: ", results)

    return total_count, entities


def get_yesterday_date():
    """
        Gets the yesturday's date.
    """
    
    today = datetime.today().astimezone().astimezone(timezone.utc)
    yest = today - timedelta(days=1, minutes=60)
    print("Today's date: %s and yestruday's date: %s" % (today, yest))

    return  yest


def get_mongodb_collection(col_name):
    """
        Returns a given collection in the database
    """
    
    mongo_client = MongoClient(MONGO_URL)
    db = mongo_client[DATABASE_NAME]
    col = db[col_name]
    
    return col