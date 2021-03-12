import json
import datetime
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import csv
from io import BytesIO
import requests

# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)


resultData = []
yelp_limit = 50
# Supported Cuisines
cuisines = ['indian', 'mexican', 'chinese', 'thai', 'italian']
# Change this location to Brooklyn, Manhattan.
locations = ["manhattan", "brooklyn"]
restaurantIterations = 20
for cuisine in cuisines:
    for i in range(restaurantIterations):
        for loc in locations:
            requestData = {
                        "term": cuisine + " restaurants",
                        "location": loc,
                        "limit": yelp_limit,
                        "offset": 50*i
                    }
            yelp_rest_endpoint = "https://api.yelp.com/v3/businesses/search"
            querystring = requestData
            payload = ""
            headers = {
                "Authorization": "Bearer nCYW8rmI0faj6QoXyvWl2O3-tdFZ9tdmNefsXcHk9xPkMuJ49PlE2QsV2w7yHG-EhBwGmaaMxy5TCLg3ELKUi4zeBqRSqSXu0kthvFfYhVFEp-gmApwLQxpdLtI-YHYx",
                'cache-control': "no-cache"
            }
            response = requests.get(yelp_rest_endpoint, data=payload, headers=headers, params=querystring)
            message = json.loads(response.text)
            print(message)
            try:
                result = message['businesses']
                resultData = resultData + result
            except:
                pass



    
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')


for restaurant in resultData:
    tableEntry = {
        'id': restaurant['id'],
        'alias': restaurant['alias'],
        'name': restaurant['name'],
        'is_closed': restaurant['is_closed'],
        'categories': restaurant['categories'],
        'rating': int(restaurant['rating']),
        'review_count': int(restaurant['review_count']),
        'address': restaurant['location']['display_address']
    }
    if (restaurant['coordinates'] and restaurant['coordinates']['latitude'] and restaurant['coordinates']['longitude']):
        tableEntry['latitude'] = str(restaurant['coordinates']['latitude'])
        tableEntry['longitude'] = str(restaurant['coordinates']['longitude'])
    if (restaurant['location']['zip_code']):
        tableEntry['zip_code'] = restaurant['location']['zip_code']
    # Add necessary attributes to the yelp-restaurants table
    table.put_item(
        Item={
            'insertedAtTimestamp': str(datetime.datetime.now()),
            'id': tableEntry['id'],
            'name': tableEntry['name'],
            'address': tableEntry['address'],
            'latitude': tableEntry.get('latitude', None),
            'longitude': tableEntry.get('longitude', None),
            'review_count': tableEntry['review_count'],
            'rating': tableEntry['rating'],
            'zip_code': tableEntry.get('zip_code', None),
            'categories': tableEntry['categories']
            }
        )



host = 'search-cloud-elastic-search-ybmh47fjqd7qokijii7kfebh74.us-east-1.es.amazonaws.com'

# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', 'es', session_token=credentials.token)
es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = ('username', 'password'),
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)
for restaurant in resultData:
    index_data = {
        'id': restaurant['id'],
        'categories': restaurant['categories']
    }                            
    print ('dataObject', index_data)
    es.index(index="restaurants", doc_type="Restaurant", id=restaurant['id'], body=index_data, refresh=True)
