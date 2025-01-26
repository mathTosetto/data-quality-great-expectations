import json
import pytest
import pandas as pd

from unittest.mock import patch
from src.great_expectations.expectations import GreatExpectationsChecker


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


@patch("src.great_expectations.expectations.gx.get_context")
def test_initilization(mock_get_context, mock_df):
    context_mode = "mock_mode"
    mock_context = mock_get_context.return_value

    result = GreatExpectationsChecker(mock_df, context_mode)

    assert isinstance(result.df, pd.DataFrame)
    assert len(result.df) == 2
    assert result.context == mock_context
    mock_get_context.assert_called_once_with(mode=context_mode) 


# def test_set_data_source():
    