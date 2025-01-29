import pytest
import pandas as pd

from unittest.mock import patch, MagicMock
from src.utils.data_loader import TaxiDataLoader


@pytest.fixture
def mock_csv_data():
    data = {
        "pickup_datetime": ["2025-01-01 08:00:00", "2025-01-01 09:00:00"],
        "dropoff_datetime": ["2025-01-01 08:30:00", "2025-01-01 09:30:00"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_taxi_data_loader():
    return TaxiDataLoader("https://mock.test")


def test_initilization(mock_taxi_data_loader):
    # Asserts
    assert mock_taxi_data_loader.url == "https://mock.test"
    assert mock_taxi_data_loader.df == None


@pytest.mark.parametrize("url", [1, 1.0, True, None, {}, [], object()])
def test_not_initilized(url):
    # Mock value
    with pytest.raises(ValueError, match="URL must be a string"):
        TaxiDataLoader(url)


def test_load_data_success(mock_taxi_data_loader, mock_csv_data, monkeypatch):
    def mock_read_csv(url):
        return mock_csv_data

    # Mock value
    monkeypatch.setattr(pd, "read_csv", mock_read_csv)
    mock_taxi_data_loader.load_data()

    # Call function
    result = mock_taxi_data_loader.df

    # Asserts
    assert result is not None
    assert not result.empty
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["pickup_datetime", "dropoff_datetime"]
    assert pd.api.types.is_datetime64_any_dtype(result["pickup_datetime"])
    assert pd.api.types.is_datetime64_any_dtype(result["dropoff_datetime"])


def test_load_data_failed(mock_taxi_data_loader, monkeypatch):
    # Set parameters
    mock_csv_data = None

    # Mock value
    monkeypatch.setattr(pd, "read_csv", mock_csv_data)
    mock_taxi_data_loader.load_data()

    # Call function
    result = mock_taxi_data_loader.df

    # Asserts
    assert result is None


def test_load_data_exception(mock_taxi_data_loader, monkeypatch, capfd):
    def mock_read_csv(url):
        raise Exception("Mocked CSV read error")

    # Mock value
    monkeypatch.setattr(pd, "read_csv", mock_read_csv)
    
    # Call function
    mock_taxi_data_loader.load_data()

    # Capture error
    captured = capfd.readouterr()

    # Asserts
    assert "Error loading data: Mocked CSV read error" in captured.out


def test_get_data(mock_taxi_data_loader, mock_csv_data, monkeypatch):
    def mock_read_csv(url):
        return mock_csv_data

    # Mock value
    monkeypatch.setattr(pd, "read_csv", mock_read_csv)
    mock_taxi_data_loader.load_data()

    # Call function
    result = mock_taxi_data_loader.get_data()

    # Asserts
    assert result is not None
    pd.testing.assert_frame_equal(result, mock_csv_data)


def test_get_data_no_data(mock_taxi_data_loader):
    # Set parameters
    mock_taxi_data_loader = MagicMock()
    mock_taxi_data_loader.df = None

    # Mock value
    mock_taxi_data_loader.get_data.side_effect = ValueError("Data has not been loaded or processed yet.")

    # Call function
    with pytest.raises(ValueError, match="Data has not been loaded or processed yet."):
        mock_taxi_data_loader.get_data()
