import os
import pytest
import pandas as pd

from unittest.mock import MagicMock, patch
from src.utils.data_loader import DataLoader


@pytest.fixture
def mock_df():
    mock_data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "date_of_birth": ["1995-01-01", "1990-05-12", "1985-07-24"],
    }
    return pd.DataFrame(mock_data)


@pytest.fixture
def mock_create_engine():
    with patch("sqlalchemy.create_engine") as mock_create_engine:
        mock_create_engine.return_value = MagicMock()
        yield mock_create_engine


@pytest.fixture
def mock_db_connection_url():
    # Parameters
    mock_connection_string = "mock_db_string_connection"

    with patch.dict(os.environ, {"CONNECTION_STRING": mock_connection_string}):
        yield mock_connection_string


@pytest.fixture
def mock_logger():
    with patch("src.utils.data_loader.logger") as mock_logger:
        yield mock_logger


@patch("src.utils.data_loader.LoggerSetup")
def test_initilization(
    mock_logger_setup, mock_db_connection_url, mock_create_engine, mock_df
):
    # Call function
    result = DataLoader(mock_df)

    # Asserts
    mock_logger_setup.assert_called_once()
    mock_create_engine.assert_called_once_with(mock_db_connection_url)
    assert isinstance(result.df, pd.DataFrame)
    assert len(result.df) == 3
    assert result.engine is not None
    assert result.engine is mock_create_engine.return_value


@pytest.mark.parametrize("df", [1, 1.0, True, None, {}, [], object()])
def test_not_initilized(mock_logger, df):
    # Mock value
    with pytest.raises(ValueError, match="The input value is not a Dataframe."):
        DataLoader(df)

    # Asserts
    mock_logger.error.assert_any_call("The input value is not a Dataframe.")


@patch("pandas.DataFrame.to_sql")
@patch("builtins.print")
def test_write_to_sql(
    mock_print, mock_to_sql, mock_logger, mock_create_engine, mock_df
):
    # Parameters
    table_name = "mock_table_name"

    # Mocks
    mock_engine = mock_create_engine.return_value
    mock_to_sql.return_value = None

    # Call function
    data_loader = DataLoader(mock_df)
    data_loader.write_to_sql(table_name=table_name, index=False)

    # Asserts
    mock_to_sql.assert_called_once_with(name=table_name, con=mock_engine, index=False)
    mock_logger.info.assert_called_once_with(f"3 rows written to {table_name}.")
    mock_print.assert_called_once_with(f"3 rows written to {table_name}.")
