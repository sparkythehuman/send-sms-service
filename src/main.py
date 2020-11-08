import boto3
import csv
import os
from boto3.dynamodb.conditions import And, Attr, Key
from uuid import uuid4
from datetime import datetime
from pytz import timezone


table = boto3.resource('dynamodb').Table(os.environ['TABLE_NAME'])

def _get_username(context):
    return context['authorizer']['claims']['cognito:username']


def _get_phone_numbers_from_file(file):
    for row in csv.DictReader(file.read().decode('utf-8-sig').split("\n")):
        yield row


def _find(username, **kwargs):
    query_params = {
        'KeyConditionExpression': Key('username').eq(username),
    }
    if kwargs:
        filter_condition_expressions = [ Attr(key).eq(value) for key, value in kwargs.items() ]
        if len(filter_condition_expressions) > 1:
            query_params['FilterExpression'] = And(*filter_condition_expressions)
        elif len(filter_condition_expressions) == 1:
            query_params['FilterExpression'] = filter_condition_expressions[0]
    
    data = table.query(**query_params)

    return data['Items']


def _response(phone_numbers):
    return {
        'statusCode': 200,
        'phone_numbers': phone_numbers
    }


def _get(event, context):
    phone_numbers = _find(
        username=_get_username(context),
        contact_list_id=event['contact-list-id']
    )

    return _response(phone_numbers)


def _create(event, context):
    # first delete all phone numbers from the contact_list
    for phone_number in _find(
        username=_get_username(context),
        contact_list_id=event['contact-list-id']
    ):
        table.delete_item(Key=phone_number['id'])

    # then add the new phone numbers to to the contact_list
    for phone_number in _get_phone_numbers_from_file(event['phone-numbers-csv']):
        table.put_item(Item={
            'id': 'PHN' + str(uuid4().int)[0:16],
            'contact_name': phone_number['name'],
            'phone_number': phone_number['phone_number'],
            'contact_list_id': event['contact-list-id'],
            'username': _get_username(context), 
            'created_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
            'updated_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
        })
        
    # return the new contact_list of phone numbers
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
    