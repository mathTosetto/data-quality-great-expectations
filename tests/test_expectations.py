import pytest
import pandas as pd

from enum import Enum
from typing import Dict
from unittest.mock import patch, MagicMock
from src.great_expectations.expectations import GreatExpectationsChecker


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
    data = [
        {
            "vendor_id": 1,
            "pickup_datetime": 1547523372000,
            "dropoff_datetime": 1547523739000,
            "passenger_count": 1,
            "trip_distance": 1.0,
            "rate_code_id": 1,
            "store_and_fwd_flag": "N",
            "pickup_location_id": 230,
            "dropoff_location_id": 48,
            "payment_type": 1,
            "fare_amount": 6.5,
            "extra": 0.5,
            "mta_tax": 0.5,
            "tip_amount": 1.95,
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.3,
            "total_amount": 9.75,
            "congestion_surcharge": None,
        },
        {
            "vendor_id": 1,
            "pickup_datetime": 1548440432000,
            "dropoff_datetime": 1548440815000,
            "passenger_count": 1,
            "trip_distance": 0.8,
            "rate_code_id": 1,
            "store_and_fwd_flag": "N",
            "pickup_location_id": 112,
            "dropoff_location_id": 112,
            "payment_type": 1,
            "fare_amount": 6.0,
            "extra": 1.0,
            "mta_tax": 0.5,
            "tip_amount": 1.55,
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.3,
            "total_amount": 9.35,
            "congestion_surcharge": 0.0,
        },
    ]
    return pd.DataFrame(data)


@pytest.fixture
def mock_get_context():
    with patch("src.great_expectations.expectations.gx.get_context") as mock:
        mock_context = MagicMock()
        mock.return_value = mock_context
        yield mock


def test_initilization(mock_get_context, mock_df, mock_config):
    # Mock value
    mock_context = mock_get_context.return_value

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)

    # Asserts
    assert isinstance(result.df, pd.DataFrame)
    assert len(result.df) == 2
    assert result.context == mock_context
    mock_get_context.assert_called_once_with(mode=mock_config.CONTEXT_MODE)


def test_set_data_source(mock_get_context, mock_df, mock_config):
    # Mock value
    mock_context = mock_get_context.return_value
    # Mock instance
    mock_context.data_sources.add_or_update_pandas = MagicMock()
    expected_value = MagicMock()
    # Mock value
    mock_context.data_sources.add_or_update_pandas.return_value = expected_value

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.set_data_source(mock_config.DATA_SOURCE)

    # Asserts
    mock_context.data_sources.add_or_update_pandas.assert_called_once_with(
        mock_config.DATA_SOURCE
    )
    assert result.data_source == expected_value


def test_set_data_asset(mock_get_context, mock_df, mock_config):
    # Mock instance
    mock_data_source = MagicMock()
    mock_data_source.add_dataframe_asset = MagicMock()
    # Mock value
    mock_get_context.data_sources.add_or_update_pandas.return_value = mock_data_source

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.data_source = mock_data_source
    result.set_data_asset(mock_config.DATA_ASSET)

    # Asserts
    mock_data_source.add_dataframe_asset.assert_called_once_with(
        name=mock_config.DATA_ASSET
    )
    assert result.data_asset == mock_data_source.add_dataframe_asset.return_value


def test_set_data_docs_site_try_branch(mock_get_context, mock_df, mock_config):
    # Mock value
    mock_context = mock_get_context.return_value
    # Mock instance
    mock_context.add_data_docs_site = MagicMock()

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.set_data_docs_site(mock_config.SITE_NAME, mock_config.SITE_CONFIG)

    # Asserts
    mock_context.add_data_docs_site.assert_called_once_with(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )


def test_set_data_docs_site_except_branch(mock_get_context, mock_df, mock_config):
    # Mock value
    mock_get_context.add_data_docs_site = MagicMock(
        side_effect=Exception("Test exception")
    )
    # Mock instance
    mock_get_context.update_data_docs_site = MagicMock()
    
    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.context = mock_get_context
    result.set_data_docs_site(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )

    # Asserts
    mock_get_context.update_data_docs_site.assert_called_once_with(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )
    mock_get_context.add_data_docs_site.assert_called_once_with(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )


def test_set_batch_definition(mock_get_context, mock_df, mock_config):
    # Mock instance)
    mock_data_asset = MagicMock()
    mock_data_asset.set_data_asset = MagicMock()
    # Mock value
    mock_get_context.data_source.add_dataframe_asset.return_value = mock_data_asset

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.data_asset = mock_data_asset
    result.set_batch_definition(mock_config.BATCH_DEFINITION)

    # Asserts
    mock_data_asset.add_batch_definition_whole_dataframe(
        name=mock_config.BATCH_DEFINITION
    )
    assert result.batch_definition == mock_data_asset.add_batch_definition_whole_dataframe.return_value


def test_set_batch(mock_get_context, mock_df, mock_config):
    # Set parameters
    batch_parameters = {"mock": mock_df}
    
    # Mock instance
    mock_batch_definition = MagicMock()
    mock_batch_definition.batch_definition = MagicMock()
    # Mock value
    mock_get_context.data_asset.add_batch_definition_whole_dataframe.return_value = mock_batch_definition

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.batch_definition = mock_batch_definition
    result.set_batch(batch_parameters=batch_parameters)

    # Asserts
    assert result.batch == mock_batch_definition.get_batch.return_value
    mock_batch_definition.get_batch(batch_parameters=batch_parameters)


def test_set_suite_existing_suite(mock_get_context, mock_df, mock_config):
    # Mock value
    mock_context = mock_get_context.return_value

    # Mock instance
    mock_suite = MagicMock()
    mock_context.suites.get.return_value = mock_suite

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.set_suite(suite_name=mock_config.SUITE_NAME)

    # Asserts
    assert result.suite == mock_suite
    mock_context.suites.get.assert_called_once_with(mock_config.SUITE_NAME)
    mock_context.suites.add_or_update.assert_not_called()
    mock_context.suites.add.assert_not_called()


@patch("great_expectations.core.expectation_suite.ExpectationSuite")
def test_set_suite_new_suite(mock_expectation_suite, mock_get_context, mock_df, mock_config):
    # Mock instance
    mock_new_suite = MagicMock()
    
    # Mock value
    mock_context = mock_get_context.return_value
    mock_context.suites.get.return_value = None
    mock_expectation_suite.return_value = mock_new_suite
    mock_context.suites.add_or_update.return_value = mock_new_suite

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.set_suite(mock_config.SUITE_NAME)

    # Assertions
    mock_context.suites.get.assert_called_once_with(mock_config.SUITE_NAME)
    mock_context.suites.add_or_update.assert_called_once()
    assert result.suite == mock_new_suite


@patch("great_expectations.core.expectation_suite.ExpectationSuite")
def test_set_suite_exception_handling(mock_expectation_suite, mock_get_context, mock_df, mock_config):
    # Mock instance
    mock_new_suite = MagicMock()
    mock_expectation_suite.side_effect = ValueError("name must be provided as a non-empty string")

    # Mock value
    mock_context = mock_get_context.return_value
    mock_context.suites.get.side_effect = Exception("Suite retrieval failed")
    mock_context.suites.add.return_value = mock_new_suite

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.context = mock_context

    # Throw error
    with pytest.raises(ValueError, match="name must be provided as a non-empty string"):
        result.set_suite(mock_config.SUITE_NAME)

    # Assertions
    mock_context.suites.get.assert_called_once_with(mock_config.SUITE_NAME)
    mock_context.suites.add.assert_not_called() 



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
    mock_columns_match
):
    # Mock instance
    mock_suite = MagicMock()
    result = MagicMock()
    result.suite = mock_suite

    # Mock values
    mock_columns_match.return_value = MagicMock()
    mock_column_type.return_value = MagicMock()
    mock_not_null.return_value = MagicMock()
    mock_between.return_value = MagicMock()
    mock_in_set.return_value = MagicMock()
    mock_row_count.return_value = MagicMock()

    # Call function
    GreatExpectationsChecker.create_expectations(result)

    # Asserts
    mock_suite.add_expectation.assert_any_call(mock_columns_match.return_value)
    mock_suite.add_expectation.assert_any_call(mock_column_type.return_value)
    mock_suite.add_expectation.assert_any_call(mock_not_null.return_value)
    mock_suite.add_expectation.assert_any_call(mock_between.return_value)
    mock_suite.add_expectation.assert_any_call(mock_in_set.return_value)
    mock_suite.add_expectation.assert_any_call(mock_row_count.return_value)
    assert mock_suite.add_expectation.call_count == 10


def test_generate_data_docs(mock_get_context, mock_df, mock_config):
    # Mock value
    mock_context = mock_get_context

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.context = mock_get_context
    result.generate_data_docs(site_name=mock_config.SITE_NAME)

    # Asserts
    mock_context.build_data_docs.assert_called_once_with(site_names=mock_config.SITE_NAME)


def test_open_report(mock_get_context, mock_df, mock_config):
    # Mock value
    mock_context = mock_get_context

    # Call function
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.open_report()

    # Asserts
    mock_context.open_data_docs.assert_called_once()
