import pytest


@pytest.fixture(autouse=True)
def set_up(monkeypatch):
    monkeypatch.setenv('TABLE_NAME', 'test-table')