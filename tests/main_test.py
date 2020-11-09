import io
import pytest
import uuid
from src.main import handle
from datetime import datetime
from pytz import timezone


@pytest.fixture
def mock_dynamodb(mocker):
    yield mocker.Mock()


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
    mock_uuid4.return_value = uuid.UUID('77f1df52-4b43-11e9-910f-b8ca3a9b9f3e')
    yield mock_uuid4


@pytest.fixture(autouse=True)
def mock_boto(mocker, mock_s3, mock_dynamodb):
    mock_boto = mocker.patch('src.main.boto3')
    mock_boto.resource.return_value = mock_dynamodb
