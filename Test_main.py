import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import requests
import pyodbc
from Main import ApiCall, LoadData


@pytest.fixture
def mock_response():
    return {
        'first_name': 'John',
        'last_name': 'Smith',
        'gender': 'Male',
        'address': {
            'country': 'USA'
        }
    }


@patch('requests.get')
def test_make_call_success(mock_get, mock_response):
    # Test to confirm that a dataframe is created with a successful Api call
    mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
    api_call = ApiCall('https://random-data-api.com/api/users/random_user?size=100')
    api_call.make_call()

    assert api_call.df is not None


@patch('requests.get')
def test_make_call_failure(mock_get):
    # Test to confirm that a dataframe is not created with an unsuccessful Api call
    mock_get.return_value = MagicMock(status_code=400)
    api_call = ApiCall('https://random-data-api.com/api/users/random_user?size=100')
    api_call.make_call()

    assert api_call.df is None


def test_format_response(mock_response):
    # This checks that the class correctly formats the API response into the dict_items attribute.
    api_call = ApiCall('https://random-data-api.com/api/users/random_user?size=100')
    api_call.format_response([mock_response])

    assert len(api_call.dict_items) == 1
    assert api_call.dict_items[0]['first_name'] == 'John'
    assert api_call.dict_items[0]['last_name'] == 'Smith'
    assert api_call.dict_items[0]['gender'] == 'Male'
    assert api_call.dict_items[0]['country'] == 'USA'


def test_create_df(mock_response):
    # Checks the create_df method of the ApiCall class correctly creates a DataFrame from the dict_items attribute
    api_call = ApiCall('https://random-data-api.com/api/users/random_user?size=100')
    api_call.dict_items = [{
        'first_name': 'John',
        'last_name': 'Smith',
        'gender': 'Male',
        'country': 'USA'
    }]
    api_call.create_df()

    assert isinstance(api_call.df, pd.DataFrame)
    assert len(api_call.df) == 1
    assert api_call.df.iloc[0]['first_name'] == 'John'
    assert api_call.df.iloc[0]['last_name'] == 'Smith'
    assert api_call.df.iloc[0]['gender'] == 'Male'
    assert api_call.df.iloc[0]['country'] == 'USA'


@pytest.fixture
def mock_data_frame():
    return pd.DataFrame({
        'first_name': ['John'],
        'last_name': ['Smith'],
        'gender': ['Male'],
        'country': ['USA']
    })


@pytest.fixture
def mock_cursor():
    return MagicMock()


@pytest.fixture
def mock_conn(mock_cursor):
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


def test_load_to_azure(mock_data_frame, mock_conn, mock_cursor):
    # Verifies that  correctly inserts data into an SQL Server database using pyodbc.
    # Mocks the pyodbc connection and cursor objects to assert the expected SQL execution and commit calls.
    with patch('pyodbc.connect', return_value=mock_conn):
        load_data = LoadData(mock_data_frame)
        load_data.load_to_azure()

        expected_execute_calls = [
            MagicMock(),
        ]
        expected_execute_calls[0].assert_called_with(
            "INSERT INTO dbo.api_data (first_name, last_name, gender, country) VALUES (?, ?, ?, ?)",
            'John', 'Smith', 'Male', 'USA'
        )

        mock_conn.commit.assert_called_once()
