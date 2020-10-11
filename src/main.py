import boto3
import csv
import os
from uuid import uuid4
from datetime import datetime
from pytz import timezone


def get_sms_from_file(record):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=record['s3']['bucket']['name'], Key=record['s3']['object']['key'])
    for row in csv.DictReader(response['Body'].read().decode('utf-8-sig').split("\n")):
        yield row


def queue_sms(sms):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    table.put_item(Item={
        'id': 'SMS' + str(uuid4().int)[0:16],
        'from': sms['from'],
        'to': sms['to'],
        'send_at': sms['send_at'], # TODO: convert to America/Denver if in other TZ
        'message': sms['message'],
        'created_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
        'status': 'queued'
    })


def handle(event, _=None):
    for record in event['Records']:
      [queue_sms(sms) for sms in get_sms_from_file(record)]