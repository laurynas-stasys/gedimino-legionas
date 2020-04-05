import argparse
import json
import pandas as pd
import urllib.request
from datetime import datetime
from sqlalchemy import create_engine
from pymongo import MongoClient


def scrape_data(api_url, entity, db_id):
    """

    :param api_url:
    :param entity:
    :param db_id:
    :return:
    """
    html = urllib.request.urlopen(api_url).read()
    data = json.loads(html)

    data = data['features']

    for ix in range(len(data)):
        data[ix]['insert_timestamp'] = datetime.now().__str__()

    entity_collection = MongoClient()[db_id][entity]
    entity_collection.insert_many(data)


def run_scraper(entity):
    """

    :param entity:
    :return:
    """
    with open('config.json', 'r') as in_str:
        config = json.loads(in_str.read())

    db_info = config['db_info']
    db = db_info['db']

    scrape_data(config['api_urls'][entity], entity, db)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("entity")
    args = parser.parse_args()

    run_scraper(args.entity)
