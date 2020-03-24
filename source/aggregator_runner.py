import argparse
import json

import pandas as pd
from sqlalchemy import create_engine


def aggregate_data(query, entity, db_host, username, password, db_id):
    """

    :param query:
    :param entity:
    :param db_host:
    :param username:
    :param password:
    :param db_id:
    :return:
    """
    conn_string = 'postgresql+psycopg2://{}:{}@{}:5432/{}'.format(username, password, db_host, db_id)
    engine = create_engine(conn_string)

    df = pd.read_sql_query(query, engine)

    diffs = df.groupby(['hospital'])[df.columns[2:-1]].diff(1).fillna(0)
    diffs.columns = ['{}_diff'.format(_) for _ in diffs.columns]

    df = pd.concat([df, diffs], axis=1).fillna(0)
    df.iloc[:, 2:] = df.iloc[:, 2:].astype(int)
    df.to_csv('{}.csv'.format(entity), index=False)


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

    aggregate_data(config['aggregate_queries'][entity], entity, host, username, password, db)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("entity")
    args = parser.parse_args()

    run_scraper(args.entity)
