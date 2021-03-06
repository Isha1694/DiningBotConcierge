import json
import boto3
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
import random
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    sqsclient = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/006767371987/SuggestionsQueue'

    # Receive message from SQS queue
    response = sqsclient.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SequenceNumber'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=1
    )
    logger.debug(response)
    
    if 'Messages' in response:
        host = 'search-restaurants-index-xir27i3go3jm4flytc7b6srx7m.us-east-1.es.amazonaws.com'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', 'es', session_token=credentials.token)
        es = Elasticsearch(
            hosts = [{'host': host, 'port': 443}],
            http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('yelp-restaurants')

        for message in response['Messages']:
            receipt_handle = message['ReceiptHandle']
            req_attributes = message['MessageAttributes']
            print req_attributes

            index_category = req_attributes['Cuisine']['StringValue']

            searchData = es.search(index="restaurants", body={
                                        "query": {
                                        "match": {
                                        "categories.title": index_category
                                        }}})

            restaurantIds = []
            for hit in searchData['hits']['hits']:
                restaurantIds.append(hit['_source']['id'])

            randomRestaurantIds = random.sample(restaurantIds, k=3)
            logger.debug("restaurantIds - %s", randomRestaurantIds)

            getMessageContent = getDynamoDbData(table, req_attributes, randomRestaurantIds)
            sendMessageToUser(req_attributes, getMessageContent)

            sqsclient.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            
            print searchData['hits']['total']


    else:
        return {
        'statusCode': 500,
        'body': json.dumps('Error fetching data from the queue.')
    }
    
    return {
        'statusCode': 200,
        'body': response
    }

def getDynamoDbData(table, requestData, businessIds):
    if len(businessIds) <= 0:
        return 'We can not find any restaurant under this description, please try again.'

    textString = "Hello! Here are my " + requestData['Categories']['StringValue'] + " restaurant suggestions for " + requestData['PeopleNum']['StringValue'] +" people, for " + requestData['DiningDate']['StringValue'] + " at " + requestData['DiningTime']['StringValue'] + ":"
    count = 1
    
    for business in businessIds:
        responseData = table.query(KeyConditionExpression=Key('id').eq(business))
        if responseData and len(responseData['Items']) >= 1:
            print responseData
            responseData = responseData['Items'][0]
            address = responseData['address'] 
            textString = textString + ", " + str(count) + ". " + str(responseData['name']) + ", located at " + str(address[0]) + " " + str(address[1])
            count+=1
    return textString

def sendMessageToUser(requestData, content):
    
    try:
        client = boto3.client('sns', region_name= 'us-east-1')
        response = client.publish(
            TopicArn = 'arn:aws:sns:us-east-1:006767371987:DiningSuggestions',
            #PhoneNumber=requestData['PhoneNum']['StringValue'],
            Message= content,
            MessageStructure='string'
        )
    except KeyError:
        logger.debug("Error sending ")
    logger.debug("response - %s",json.dumps(response) )
    logger.debug("Message = '%s' Phone Number = %s" % (content, requestData['PhoneNum']['StringValue']))