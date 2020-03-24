import argparse
import json
import pandas as pd
import urllib.request
from datetime import datetime
from sqlalchemy import create_engine


def scrape_data(api_url, entity, db_host, username, password, db_id):
    """

    :param api_url:
    :param entity:
    :param db_host:
    :param username:
    :param password:
    :param db_id:
    :return:
    """
    html = urllib.request.urlopen(api_url).read()
    data = json.loads(html)

    current_time = datetime.now().__str__()

    df = pd.DataFrame.from_dict([_['attributes'] for _ in data['features']]).dropna(axis=1)
    df['insert_timestamp'] = current_time

    conn_string = 'postgresql+psycopg2://{}:{}@{}:5432/{}'.format(username, password, db_host, db_id)

    engine = create_engine(conn_string)
    df.to_sql(entity, engine, if_exists='append')


def run_scraper(entity):
    """

    :param entity:
    :return:
    """
    with open('config.json', 'r') as in_str:
        config = json.loads(in_str.read())

    db_info = config['db_info']
    host = db_info['db_host']
    username = db_info['username']
    password = db_info['password']
    db = db_info['db']

    scrape_data(config['api_urls'][entity], entity, host, username, password, db)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("entity")
    args = parser.parse_args()

    run_scraper(args.entity)
