import pytest
import pandas as pd

from enum import Enum
from typing import Dict
from unittest.mock import patch, MagicMock
import great_expectations.expectations as gxe
from src.great_expectations_checker.postgres_checker import (
    GreatExpectationsPostgresChecker,
)


class MockConfig(Enum):
    CONTEXT_MODE: str = "mock_mode"
    DATA_SOURCE: str = "mock_source"
    DATA_ASSET: str = "mock_data_asset"
    BATCH_DEFINITION: str = "mock_definition"
    SITE_NAME: str = "mock_site"
    SITE_CONFIG: Dict[str, str] = {"mock_key": "mock_value"}
    SUITE_NAME: str = "mock_suite"
    CONNECTION_STRING: str = "mock_connection_string"
    TABLE_NAME: str = "mock_table"
    SCHEMA: str = "mock_schema"


@pytest.fixture
def mock_config():
    return MockConfig


@pytest.fixture
def mock_get_context():
    with patch(
        "src.great_expectations_checker.base_checker.gx.get_context"
    ) as mock_get_context:
        mock_get_context.return_value = MagicMock()
        yield mock_get_context


def test_init(mock_get_context, mock_config):
    # Call function
    result = GreatExpectationsPostgresChecker(mock_config.CONTEXT_MODE)

    # Asserts
    assert result.suite == None
    assert result.batch_definition == None
    mock_get_context.assert_called_once_with(mode=mock_config.CONTEXT_MODE)
    assert result.context == mock_get_context.return_value


def test_set_data_source(mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context.return_value
    mock_context.data_sources = MagicMock()

    expected_value = MagicMock()
    mock_context.data_sources.add_or_update_postgres.return_value = expected_value

    # Call function
    result = GreatExpectationsPostgresChecker(mock_config.CONTEXT_MODE)
    result.set_data_source(mock_config.DATA_SOURCE, mock_config.CONNECTION_STRING)

    # Asserts
    assert result.context == mock_context
    mock_context.data_sources.add_or_update_postgres.assert_called_once_with(
        name=mock_config.DATA_SOURCE, connection_string=mock_config.CONNECTION_STRING
    )
    assert result.data_source == expected_value


def test_set_data_asset(mock_get_context, mock_config):
    # Mocks
    mock_data_source = MagicMock()
    mock_data_source.add_or_update_postgres = MagicMock()
    mock_get_context.data_sources.add_table_asset.return_value = mock_data_source

    # Call function
    result = GreatExpectationsPostgresChecker(mock_config.CONTEXT_MODE)
    result.data_source = mock_data_source
    result.set_data_asset(
        mock_config.DATA_ASSET, mock_config.TABLE_NAME, mock_config.SCHEMA
    )

    # Asserts
    mock_data_source.add_table_asset.assert_called_once_with(
        name=mock_config.DATA_ASSET,
        table_name=mock_config.TABLE_NAME,
        schema_name=mock_config.SCHEMA,
    )
    assert result.data_asset == mock_data_source.add_table_asset.return_value


def test_set_batch_definition(mock_get_context, mock_config):
    # Mocks
    mock_data_asset = MagicMock()
    mock_data_asset.set_data_asset = MagicMock()
    mock_get_context.data_source.add_table_asset.return_value = mock_data_asset

    # Call function
    result = GreatExpectationsPostgresChecker(mock_config.CONTEXT_MODE)
    result.data_asset = mock_data_asset
    result.set_batch_definition(mock_config.BATCH_DEFINITION)

    # Asserts
    mock_data_asset.add_batch_definition_whole_table(name=mock_config.BATCH_DEFINITION)
    assert (
        result.batch_definition
        == mock_data_asset.add_batch_definition_whole_table.return_value
    )


def test_create_expectations(mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context.return_value
    mock_suite = MagicMock()
    mock_suite.expectations = []

    # Call function
    result = GreatExpectationsPostgresChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.suite = mock_suite
    result._update_suite = MagicMock()
    result.create_expectations()

    # Asserts
    assert len(result.suite.expectations) == 1
    assert isinstance(result.suite.expectations[0], gxe.ExpectColumnValuesToBeOfType)
    assert result.suite.expectations[0].column == "vendor_id"
    assert result.suite.expectations[0].type_ == "Integer"
    result._update_suite.assert_called_once()


def test_update_suite(mock_get_context, mock_config):
    # Mocks
    mock_suite = MagicMock()
    mock_context_instance = MagicMock()
    mock_get_context.return_value = mock_context_instance

    # Call function
    result = GreatExpectationsPostgresChecker(mock_config.CONTEXT_MODE)
    result.suite = mock_suite
    result.context = mock_context_instance
    result._update_suite()

    # Asserts
    mock_context_instance.suites.add_or_update.assert_called_once_with(mock_suite)
    mock_suite.save.assert_called_once()
    mock_context_instance.build_data_docs.assert_called_once()
