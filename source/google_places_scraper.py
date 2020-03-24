import googlemaps
from os import path
from urllib import request
import json
from pymongo import MongoClient


def get_next_token_string(query):
    """

    :return:
    """
    next_token_fpath = 'next_token_{}'.format(query)

    if path.exists(next_token_fpath):
        with open(next_token_fpath, 'r') as in_str:
            next_token = in_str.read()
            return True, {'pagetoken': next_token}
    else:
        return False, {}


def update_next_token_string(token, query):
    """

    :param token:
    :param query:
    :return:
    """
    next_token_fpath = 'next_token_{}'.format(query)
    with open(next_token_fpath, 'w') as out_str:
        out_str.write(token)


def scrape_places(place_type, mongo_db):
    """

    :param place_type:
    :return:
    """
    collection = mongo_db[place_type]

    i = 0
    while True:

        parameters = {'key': 'INSERT_YOURS',
                      'types': place_type,
                      'query': '{}+vilnius+lithuania'.format(place_type)}
        next_token_exists, token = get_next_token_string(parameters['query'])

        if next_token_exists:
            parameters.update(token)

        query_params = '&'.join(['{}={}'.format(k, v) for (k, v) in parameters.items()])
        query_url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?{}'.format(query_params)

        print(query_params)
        response = request.urlopen(query_url).read()

        data = json.loads(response)

        results = data['results']

        if 'next_page_token' in data:
            next_token = data['next_page_token']
            collection.insert_many(results)
            update_next_token_string(next_token, parameters['query'])
            print(i, next_token)
            i += 1

            import time
            time.sleep(10)
        else:
            break


if __name__ == '__main__':

    mongo = MongoClient()
    places_db = mongo.get_database('places')

    scrape_places('point_of_interest', places_db)
