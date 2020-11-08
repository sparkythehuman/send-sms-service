import boto3
import csv
import os
from boto3.dynamodb.conditions import Key, And
from uuid import uuid4
from datetime import datetime
from pytz import timezone


table = boto3.resource('dynamodb').Table(os.environ['TABLE_NAME'])

def _get_username(context):
    return context['authorizer']['claims']['cognito:username']


def _get_phone_numbers_from_file(file):
    for row in csv.DictReader(file.read().decode('utf-8-sig').split("\n")):
        yield row


def _find(**kwargs):
    key_condition_expressions = [ Key(key).eq(value) for key, value in kwargs.items() ]
    data = table.query(
        KeyConditionExpression=And(*key_condition_expressions)
    )
    return data['Items']


def _response(phone_numbers):
    return {
        'statusCode': 200,
        'phone_numbers': phone_numbers
    }


def _get(event, context):
    phone_numbers = _find(
        username=_get_username(context),
        list_id=event['list-id']
    )

    return _response(phone_numbers)


def _create(event, context):
    # first delete all phone numbers from the list
    for phone_number in _find(
        username=_get_username(context),
        list_id=event['list-id']
    ):
        table.delete_item(Key=phone_number['id'])

    # then add the new phone numbers to to the list
    for phone_number in _get_phone_numbers_from_file(event['phone-numbers-csv']):
        table.put_item(Item={
            'id': 'PHN' + str(uuid4().int)[0:16],
            'name': phone_number['name'],
            'phone_number': phone_number['phone_number'],
            'list_id': event['list-id'],
            'username': _get_username(context), 
            'created_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
            'updated_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
        })
        
    # return the new list of phone numbers
    return _get(event, context) 


def handle(event, context):
    operation = context['httpMethod']
    operations = {
        'GET' : _get,
        'POST': _create
    }
    if operation in operations:
        return operations[operation](event, context)
    else:
        raise ValueError(f'Unable to run operation for HTTP METHOD: {operation}')
    