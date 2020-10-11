import io
import pytest
from src.main import handle
from datetime import datetime
from pytz import timezone

s3_event = {
    'Records': [{
        's3': {
            'bucket': {
                'name': 'johnnyboards-sms-service-imports',
            },
            'object': {
                'key': 'test.csv',
                'eTag': '5d610d4105d13997f2430e5bd16193f9',
            }
        }
    }]
}


@pytest.fixture
def mock_dynamodb(mocker):
    yield mocker.Mock()


@pytest.fixture
def mock_s3(mocker):
    mock_s3 = mocker.Mock()
    mock_s3.get_object.return_value = {
        'Body': io.BytesIO(
            b'from,to,send_at,timezone,message\n'
            b'+15058675309,+12813308004,2020-01-01 12:30:00,America/Denver,"Join Earth\'s mightiest heroes, like Kevin Bacon."\n'
        )
    }
    yield mock_s3


@pytest.fixture
def mock_now():
    yield datetime.now(tz=timezone('America/Denver'))


@pytest.fixture(autouse=True)
def mock_datetime(mocker, mock_now):
    mock_datetime = mocker.patch('src.main.datetime')
    mock_datetime.now.return_value = mock_now


@pytest.fixture
def mock_uuid4(mocker):
    mock_uuid4 = mocker.patch('src.main.uuid4')
    mock_uuid4.int.return_value = mocker.Mock()
    yield mock_uuid4


@pytest.fixture(autouse=True)
def mock_boto(mocker, mock_s3, mock_dynamodb):
    mock_boto = mocker.patch('src.main.boto3')
    mock_boto.resource.return_value = mock_dynamodb
    mock_boto.client.side_effect = [mock_s3]


def test_main(mocker, mock_dynamodb, mock_s3, mock_now, mock_uuid4):
    handle(s3_event)

    assert mock_s3.mock_calls == [
        mocker.call.get_object(
            Bucket='johnnyboards-sms-service-imports',
            Key='test.csv',
        )
    ]

    assert mock_dynamodb.mock_calls == [
        mocker.call.Table('test-table'),
        mocker.call.Table().put_item(Item={
            'id': 'SMS' + str(mock_uuid4)[0:16],
            'status': 'queued', 
            'from': '+15058675309', 
            'created_at': mock_now.isoformat(), 
            'to': '+12813308004', 
            'send_at': '2020-01-01 12:30:00', 
            'message': "Join Earth's mightiest heroes, like Kevin Bacon."
        })
    ]