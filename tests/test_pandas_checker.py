import pytest
import pandas as pd

from enum import Enum
from typing import Dict
from unittest.mock import patch, MagicMock
from src.great_expectations_checker.pandas_checker import GreatExpectationsPandasChecker


class MockConfig(Enum):
    CONTEXT_MODE: str = "mock_mode"
    DATA_SOURCE: str = "mock_source"
    DATA_ASSET: str = "mock_data_asset"
    BATCH_DEFINITION: str = "mock_definition"
    SITE_NAME: str = "mock_site"
    SITE_CONFIG: Dict[str, str] = {"mock_key": "mock_value"}
    SUITE_NAME: str = "mock_suite"


@pytest.fixture
def mock_config():
    return MockConfig


@pytest.fixture
def mock_df():
    data = {
        "vendor_id": [1, 2, 3],
        "pickup_datetime": [
            "2023-01-01 10:00:00",
            "2023-01-02 10:00:00",
            "2023-01-03 10:00:00",
        ],
        "dropoff_datetime": [
            "2023-01-01 10:30:00",
            "2023-01-02 10:30:00",
            "2023-01-03 10:30:00",
        ],
        "passenger_count": [1, 2, 3],
        "trip_distance": [5.0, 7.5, 6.0],
        "rate_code_id": [1, 2, 1],
        "store_and_fwd_flag": ["Y", "N", "Y"],
        "pickup_location_id": [100, 101, 102],
        "dropoff_location_id": [200, 201, 202],
        "payment_type": [1, 2, 1],
        "fare_amount": [10.0, 15.0, 12.5],
        "extra": [0.5, 0.5, 0.5],
        "mta_tax": [0.5, 0.5, 0.5],
        "tip_amount": [2.0, 3.0, 2.5],
        "tolls_amount": [1.0, 1.0, 1.0],
        "improvement_surcharge": [0.3, 0.3, 0.3],
        "total_amount": [20.3, 25.3, 23.8],
        "congestion_surcharge": [1.0, 1.0, 1.0],
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_get_context():
    with patch(
        "src.great_expectations_checker.base_checker.gx.get_context"
    ) as mock_get_context:
        mock_get_context.return_value = MagicMock()
        yield mock_get_context


def test_init(mock_get_context, mock_df, mock_config):
    # Call function
    result = GreatExpectationsPandasChecker(mock_df, mock_config.CONTEXT_MODE)

    # Asserts
    assert result.df is mock_df
    assert result.suite == None
    assert result.batch_definition == None
    mock_get_context.assert_called_once_with(mode=mock_config.CONTEXT_MODE)
    assert result.context == mock_get_context.return_value


def test_set_data_source(mock_get_context, mock_df, mock_config):
    # Mocks
    mock_context = mock_get_context.return_value
    mock_context.data_sources.add_or_update_pandas = MagicMock()

    expected_value = MagicMock()
    mock_context.data_sources.add_or_update_pandas.return_value = expected_value

    # Call function
    result = GreatExpectationsPandasChecker(mock_df, mock_config.CONTEXT_MODE)
    result.set_data_source(mock_config.DATA_SOURCE)

    # Asserts
    mock_context.data_sources.add_or_update_pandas.assert_called_once_with(
        mock_config.DATA_SOURCE
    )
    assert result.data_source == expected_value


def test_set_data_asset(mock_get_context, mock_df, mock_config):
    # Mocks
    mock_data_source = MagicMock()
    mock_data_source.add_dataframe_asset = MagicMock()
    mock_get_context.data_sources.add_or_update_pandas.return_value = mock_data_source

    # Call function
    result = GreatExpectationsPandasChecker(mock_df, mock_config.CONTEXT_MODE)
    result.data_source = mock_data_source
    result.set_data_asset(mock_config.DATA_ASSET)

    # Asserts
    mock_data_source.add_dataframe_asset.assert_called_once_with(
        name=mock_config.DATA_ASSET
    )
    assert result.data_asset == mock_data_source.add_dataframe_asset.return_value


def test_set_batch_definition(mock_get_context, mock_df, mock_config):
    # Mocks
    mock_data_asset = MagicMock()
    mock_data_asset.set_data_asset = MagicMock()
    mock_get_context.data_source.add_dataframe_asset.return_value = mock_data_asset

    # Call function
    result = GreatExpectationsPandasChecker(mock_df, mock_config.CONTEXT_MODE)
    result.data_asset = mock_data_asset
    result.set_batch_definition(mock_config.BATCH_DEFINITION)

    # Asserts
    mock_data_asset.add_batch_definition_whole_dataframe(
        name=mock_config.BATCH_DEFINITION
    )
    assert (
        result.batch_definition
        == mock_data_asset.add_batch_definition_whole_dataframe.return_value
    )


@pytest.mark.skip(reason="Review later on")
@patch("great_expectations.expectations.ExpectTableColumnsToMatchOrderedList")
@patch("great_expectations.expectations.ExpectColumnValuesToBeOfType")
@patch("great_expectations.expectations.ExpectColumnValuesToNotBeNull")
@patch("great_expectations.expectations.ExpectColumnValuesToBeBetween")
@patch("great_expectations.expectations.ExpectColumnValuesToBeInSet")
@patch("great_expectations.expectations.ExpectTableRowCountToBeBetween")
def test_create_expectations(
    mock_row_count,
    mock_in_set,
    mock_between,
    mock_not_null,
    mock_column_type,
    mock_columns_match,
    mock_get_context,
    mock_df,
    mock_config
):
    # Mocks for the expectation classes themselves
    mock_columns_match.return_value = MagicMock()
    mock_column_type.return_value = MagicMock()
    mock_not_null.return_value = MagicMock()
    mock_between.return_value = MagicMock()
    mock_in_set.return_value = MagicMock()
    mock_row_count.return_value = MagicMock()

    # Mock the context and suite setup
    mock_context = MagicMock()
    mock_suite = MagicMock()
    mock_suite.expectations = []  # Ensure expectations is an empty list

    # Set the suite's expectations to an empty list
    mock_context.suites.get.return_value = mock_suite  # This is where the suite is retrieved
    mock_context.suites.add_or_update.return_value = mock_suite  # This ensures the suite can be updated

    # Simulate the behavior of `get_context`
    mock_get_context.return_value = mock_context

    # Create the checker instance
    checker = GreatExpectationsPandasChecker(mock_df, mock_config['CONTEXT_MODE'])

    # Call the function to test
    checker.create_expectations()

    # Assert that expectations were added to the suite
    mock_suite.add_expectation.assert_any_call(mock_columns_match.return_value)
    mock_suite.add_expectation.assert_any_call(mock_column_type.return_value)
    mock_suite.add_expectation.assert_any_call(mock_not_null.return_value)
    mock_suite.add_expectation.assert_any_call(mock_between.return_value)
    mock_suite.add_expectation.assert_any_call(mock_in_set.return_value)
    mock_suite.add_expectation.assert_any_call(mock_row_count.return_value)

    # Ensure that add_expectation is called 10 times (for the 10 expectations in create_expectations)
    assert mock_suite.add_expectation.call_count == 10


def test_update_suite(mock_get_context, mock_df, mock_config):
    # Mocks
    mock_suite = MagicMock()
    mock_context_instance = MagicMock()
    mock_get_context.return_value = mock_context_instance

    # Call function
    result = GreatExpectationsPandasChecker(mock_df, mock_config.CONTEXT_MODE)
    result.suite = mock_suite
    result.context = mock_context_instance
    result._update_suite()

    # Asserts
    mock_context_instance.suites.add_or_update.assert_called_once_with(mock_suite)
    mock_context_instance.suites.save.assert_called_once_with(mock_suite)
    mock_context_instance.build_data_docs.assert_called_once()
    mock_context_instance.suites.add_or_update.assert_called_once()
    mock_context_instance.suites.save.assert_called_once()
    mock_context_instance.build_data_docs.assert_called_once()
