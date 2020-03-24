import pymongo
from urllib import request
import json


if __name__ == '__main__':

    mongo_client = pymongo.MongoClient()
    api_key = 'INSERT_YOURS'
    places_db = mongo_client.get_database('places')

    for collection in places_db.list_collections():

        for place_id in places_db[collection['name']].find({}, {'place_id': 1, '_id': 0}):

            query_url = 'https://maps.googleapis.com/maps/api/place/details/json?place_id={}&key={}'.format(place_id['place_id'], api_key)

            response = request.urlopen(query_url).read()
            result = json.loads(response)['result']

            places_db\
                [collection['name']]\
                .find_one_and_update({'place_id': place_id['place_id']}, {'$set': {'meta': result}})

            print(collection['name'], place_id)