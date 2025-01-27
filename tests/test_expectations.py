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
    SITE_NAME: str = "mock_site"
    SITE_CONFIG: Dict[str, str] = {"mock_key": "mock_value"}
    SUITE_NAME = "mock_suite"


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


# @patch("src.great_expectations.expectations.gx.get_context")
def test_initilization(mock_get_context, mock_df, mock_config):
    mock_context = mock_get_context.return_value

    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)

    assert isinstance(result.df, pd.DataFrame)
    assert len(result.df) == 2
    assert result.context == mock_context
    mock_get_context.assert_called_once_with(mode=mock_config.CONTEXT_MODE)


def test_set_data_source(mock_get_context, mock_df, mock_config):
    mock_context = mock_get_context.return_value
    mock_context.data_sources.add_or_update_pandas = MagicMock()
    expected_value = MagicMock()
    mock_context.data_sources.add_or_update_pandas.return_value = expected_value

    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)

    result.set_data_source(mock_config.DATA_SOURCE)

    mock_context.data_sources.add_or_update_pandas.assert_called_once_with(
        mock_config.DATA_SOURCE
    )
    assert result.data_source == expected_value


def test_set_data_asset(mock_get_context, mock_df, mock_config):
    mock_data_source = MagicMock()
    mock_get_context.data_sources.add_or_update_pandas.return_value = mock_data_source

    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)

    mock_data_source.add_dataframe_asset = MagicMock()
    result.data_source = mock_data_source

    result.set_data_asset(mock_config.DATA_ASSET)

    mock_data_source.add_dataframe_asset.assert_called_once_with(
        name=mock_config.DATA_ASSET
    )
    assert result.data_asset == mock_data_source.add_dataframe_asset.return_value


def test_set_data_docs_site_try_branch(mock_get_context, mock_df, mock_config):
    mock_context = mock_get_context.return_value
    mock_context.add_data_docs_site = MagicMock()

    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)

    result.set_data_docs_site(mock_config.SITE_NAME, mock_config.SITE_CONFIG)

    mock_context.add_data_docs_site.assert_called_once_with(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )


@pytest.mark.skip(reason="Currently working on this")
def test_set_data_docs_site_except_branch(mock_get_context, mock_df, mock_config):
    result = GreatExpectationsChecker(mock_df, mock_config.CONTEXT_MODE)

    mock_get_context.add_data_docs_site = MagicMock(
        side_effect=Exception("Test exception")
    )
    mock_get_context.update_data_docs_site = MagicMock()

    result.set_data_docs_site(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )

    mock_get_context.update_data_docs_site.assert_called_once_with(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )
    mock_get_context.add_data_docs_site.assert_called_once_with(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )
