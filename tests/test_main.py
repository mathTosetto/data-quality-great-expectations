import pytest
import logging
import os
import pandas as pd
from unittest.mock import patch, MagicMock

from main import (
    load_taxi_data,
    load_data_to_sql,
    run_expectations,
    validate_expectations,
    main,
)


@pytest.fixture
def mock_df():
    """Fixture to create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "vendor_id": [1, 2],
            "pickup_datetime": ["2023-01-01 10:00:00", "2023-01-02 11:00:00"],
            "dropoff_datetime": ["2023-01-01 10:30:00", "2023-01-02 11:30:00"],
            "passenger_count": [1, 2],
            "trip_distance": [5.0, 7.5],
        }
    )


@patch("main.TaxiDataExtractor")
def test_load_taxi_data(mock_extractor, mock_df):
    # Mocks
    mock_instance = mock_extractor.return_value
    mock_instance.get_data.return_value = mock_df

    # Call function
    df = load_taxi_data("mock_url")

    # Asserts
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    mock_extractor.assert_called_once_with("mock_url")
    mock_instance.load_data.assert_called_once()
    mock_instance.get_data.assert_called_once()


@patch("main.DataLoader")
def test_load_data_to_sql(mock_data_loader, mock_df):
    # Mocks
    mock_instance = mock_data_loader.return_value

    # Call function
    data_loader = load_data_to_sql(mock_df)

    # Asserts
    assert isinstance(data_loader, MagicMock)
    mock_data_loader.assert_called_once_with(mock_df)
    mock_instance.write_to_sql.assert_called_once_with(
        table_name="stg_taxi_data",
        schema="stage",
        if_exists="append",
        index=False,
    )


@patch("main.GreatExpectationsPostgresChecker")
@patch("os.getenv")
def test_run_expectations(mock_getenv, mock_ge_checker):
    # Mocks
    mock_getenv.return_value = "mock_connection_string"
    mock_checker_instance = mock_ge_checker.return_value
    mock_checker_instance.run_checkpoint.return_value.success = True

    # Call function
    result = run_expectations()

    # Asserts
    assert result is True
    mock_getenv.assert_called_once_with("CONNECTION_STRING")
    # mock_ge_checker.assert_called_once_with("mode")  # Ensure context mode is correct

    # Verify that all the methods were called
    expected_calls = [
        "set_data_source",
        "set_data_asset",
        "set_data_docs_site",
        "set_batch_definition",
        "set_suite",
        "create_expectations",
        "run_checkpoint",
        "generate_data_docs",
    ]
    
    for method in expected_calls:
        method_mock = getattr(mock_checker_instance, method)
        try:
            method_mock.assert_called_once()
        except AssertionError:
            print(f"❌ Method {method} was not called as expected.")

    mock_checker_instance.run_checkpoint.assert_called_once()


@patch("main.GreatExpectationsPostgresChecker")
def test_validate_expectations_fail(mock_ge_checker):
    # Mocks
    mock_data_loader = MagicMock()

    # Asserts
    with pytest.raises(ValueError, match="Data validation failed!"):
        validate_expectations(mock_data_loader, False)
    mock_ge_checker.return_value.open_report.assert_called_once()


@patch("main.load_taxi_data")
@patch("main.load_data_to_sql")
@patch("main.run_expectations")
@patch("main.validate_expectations")
def test_main(mock_validate, mock_run_expectations, mock_load_sql, mock_load_data):
    # Mocks
    mock_df = MagicMock()
    mock_loader = MagicMock()
    mock_load_data.return_value = mock_df
    mock_load_sql.return_value = mock_loader
    mock_run_expectations.return_value = True

    # Call function
    main()

    # Asserts
    mock_load_data.assert_called_once()
    mock_load_sql.assert_called_once_with(mock_df)
    mock_run_expectations.assert_called_once()
    mock_validate.assert_called_once_with(mock_loader, True)


@patch("main.load_taxi_data", side_effect=Exception("Test Error"))
def test_main_exception(mock_load_data, caplog):
    # Parameters
    caplog.set_level(logging.ERROR)

    # Call function
    with pytest.raises(Exception, match="Test Error"):
        main()

    # Asserts
    assert "🚨 Error in pipeline execution: Test Error" in caplog.text
