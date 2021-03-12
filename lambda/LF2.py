import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
TABLE_NAME = 'yelp-restaurants'
SAMPLE_N = '5'
host = 'https://search-cloud-elastic-search-ybmh47fjqd7qokijii7kfebh74.us-east-1.es.amazonaws.com'
queue_url = 'https://sqs.us-east-1.amazonaws.com/089149523310/Cloudqueue'
# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', 'es',session_token=credentials.token)
sqsclient = boto3.client('sqs',region_name='us-east-1')
es = Elasticsearch(
            host,
            http_auth = ('username', 'password'),
            connection_class = RequestsHttpConnection
            )
#check = es.get(index="restaurants", doc_type="Restaurant", id='-KahGyU9G7JT0JmoC_Yc0Q')
#print(check)


def sendsms(number, message):
    send_sms = boto3.client('sns',region_name='us-east-1')
    smsattrs = {
        'AWS.SNS.SMS.SenderID': {
            'DataType': 'String',
            'StringValue': 'TestSender'
        },
        'AWS.SNS.SMS.SMSType': {
            'DataType': 'String',
            'StringValue': 'Transactional'  # change to Transactional from Promotional for dev
        }
    }
    response = send_sms.publish(
        PhoneNumber=number,
        Message=message,
        MessageAttributes=smsattrs
    )
    print(number)
    print(response)
    print("The message is: ", message)


def search(cuisine):
    data = es.search(index="restaurants", body={"query": {"match": {'categories.title':cuisine}}})
    print(data)
    print("search complete", data['hits']['hits'])
    return data['hits']['hits']


def get_restaurant_data(ids):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')
    ans = 'Hi! Here are your suggestions,\n '
    i = 1
    for id in ids:
        if i<6:
            response = table.get_item(
                Key={
                    'id': id
                }
            )
            print(response)
            response_item = response['Item']
            print(response_item)
            restaurant_name = response_item['name']
            restaurant_address = response_item['address']
            # restaurant_city = response_item['city:']
            restaurant_zipcode = response_item['zip_code']
            restaurant_rating = str(response_item['rating'])
            ans += "{}. {}, located at {}\n".format(i, restaurant_name, restaurant_address)
            # return ans
            i += 1
        else:
            break
    print("db pass")
    return ans # string type


def lambda_handler(event=None, context=None):
    messages = sqsclient.receive_message(QueueUrl=queue_url, MessageAttributeNames=['All'])
    print(messages)
    try:
        message = messages['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        req_attributes = message['MessageAttributes']
        print(req_attributes)
        cuisine = req_attributes['Cuisine']['StringValue']
        location = req_attributes['Location']['StringValue']
        dining_date = req_attributes['DiningDate']['StringValue']
        dining_time = req_attributes['DiningTime']['StringValue']
        num_people = req_attributes['PeopleNum']['StringValue']
        phone = req_attributes['PhoneNum']['StringValue']
        print(location, cuisine, dining_date, dining_time, num_people, phone)
        print(phone)
        ids = search(cuisine)
        ids = list(map(lambda x: x['_id'], ids))
        print(ids)
        rest_details = get_restaurant_data(ids)
        sendsms("+1"+phone, rest_details)
        sqsclient.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
    except Exception as e:
        print(e)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda LF2!')
    }


lambda_handler()
