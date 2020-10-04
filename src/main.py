import boto3
import csv
import os
from datetime import datetime
from pytz import timezone


def get_sms_from_file(record):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=record['s3']['bucket']['name'], Key=record['s3']['object']['key'])
    for row in csv.DictReader(response['Body'].read().decode('utf8').split()):
        yield row


def queue_sms(sms):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    table.put_item(Item={
        'from': sms['from'],
        'to': sms['to'],
        'send_at': sms['send_at'], # any timezone stuff needed here?
        'message': sms['message'],
        'created_at': datetime.now(tz=timezone('America/Denver')).isoformat(), # always America/Denver?
        'status': 'queued'
    })


def handle(event, _=None):
    for record in event['Records']:
      [queue_sms(sms) for sms in get_sms_from_file(record)]