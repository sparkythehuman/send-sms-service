import boto3
import csv
import json
import os
from boto3.dynamodb.conditions import And, Attr, Key
from uuid import uuid4
from datetime import datetime
from pytz import timezone


contact = boto3.resource('dynamodb').Table('contact')

def _get_username(event):
    return event['requestContext']['authorizer']['claims']['cognito:username']


def _get_phone_numbers_from_file(file):
    for row in csv.DictReader(file.read().decode('utf-8-sig').split("\n")):
        yield row

def _get_phone_numbers(request):
    for row in request['contacts']:
        yield row


def _find(username, **kwargs):
    query_params = {
        'IndexName': 'username-index',
        'KeyConditionExpression': Key('username').eq(username),
    }
    if kwargs:
        filter_condition_expressions = [ Attr(key).eq(value) for key, value in kwargs.items() ]
        if len(filter_condition_expressions) > 1:
            query_params['FilterExpression'] = And(*filter_condition_expressions)
        elif len(filter_condition_expressions) == 1:
            query_params['FilterExpression'] = filter_condition_expressions[0]
    
    data = contact.query(**query_params)

    return data['Items']


def _response(phone_numbers):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(phone_numbers)
    }


def _get(request, username):
    phone_numbers = _find(
        username=username,
        contact_list_id=request['contact_list_id']
    )

    return _response(phone_numbers)


def _create(request, username):
    # first delete all contacts from the contact_list
    for _contact in _find(
        username=username,
        contact_list_id=request['contact_list_id']
    ):
        contact.delete_item(
            Key={'id': _contact['id']}
        )

    # then add the new phone numbers to to the contact_list
    print(request)
    # for _contact in _get_phone_numbers_from_file(request['phone_numbers_csv']):
    for _contact in _get_phone_numbers(request):
        contact.put_item(Item={
            'id': 'CPH' + str(uuid4().int)[0:16],
            'contact_name': _contact['name'],
            'phone_number': _contact['phone_number'],
            'contact_list_id': request['contact_list_id'],
            'username': username, 
            'created_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
            'updated_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
        })
        
    # return the new contact_list of phone numbers
    return _get(request, username) 


def handle(event, context):
    operation = event['requestContext']['httpMethod']
    operations = {
        'GET' : _get,
        'POST': _create
    }
    if operation in operations:
        print(event)
        return operations[operation](json.loads(event['body']), _get_username(event))
    else:
        raise ValueError(f'Unable to run operation for HTTP METHOD: {operation}')
    