import pytest
import pandas as pd

from enum import Enum
from typing import Dict
from unittest.mock import patch, MagicMock
from src.great_expectations_checker.base_checker import GreatExpectationsChecker


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
def mock_get_context():
    with patch(
        "src.great_expectations_checker.base_checker.gx.get_context"
    ) as mock_get_context:
        mock_get_context.return_value = MagicMock()
        yield mock_get_context


def test_init(mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context.return_value

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)

    # Asserts
    assert result.context == mock_context
    assert result.suite == None
    assert result.batch_definition == None
    mock_get_context.assert_called_once_with(mode=mock_config.CONTEXT_MODE)


def test_set_data_docs_site_try_branch(mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context.return_value

    mock_context.add_data_docs_site = MagicMock()

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.set_data_docs_site(mock_config.SITE_NAME, mock_config.SITE_CONFIG)

    # Asserts
    mock_context.add_data_docs_site.assert_called_once_with(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )


def test_set_data_docs_site_except_branch(mock_get_context, mock_config):
    # Mocks
    mock_get_context.add_data_docs_site = MagicMock(
        side_effect=Exception("Test exception")
    )
    mock_get_context.update_data_docs_site = MagicMock()

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_get_context
    result.set_data_docs_site(
        site_name=mock_config.SITE_NAME, site_config=mock_config.SITE_CONFIG
    )


def test_set_suite_existing_suite(mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context.return_value

    mock_suite = MagicMock()
    mock_context.suites.get.return_value = mock_suite

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.set_suite(suite_name=mock_config.SUITE_NAME)

    # Asserts
    assert result.suite == mock_suite
    mock_context.suites.get.assert_called_once_with(mock_config.SUITE_NAME)
    mock_context.suites.add_or_update.assert_not_called()
    mock_context.suites.add.assert_not_called()


@patch("great_expectations.core.expectation_suite.ExpectationSuite")
def test_set_suite_new_suite(mock_expectation_suite, mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context.return_value
    mock_context.suites.get.return_value = None

    mock_new_suite = MagicMock()
    mock_expectation_suite.return_value = mock_new_suite
    mock_context.suites.add_or_update.return_value = mock_new_suite

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.set_suite(mock_config.SUITE_NAME)

    # Asserts
    mock_context.suites.get.assert_called_once_with(mock_config.SUITE_NAME)
    mock_context.suites.add_or_update.assert_called_once()
    assert result.suite == mock_new_suite


@patch("great_expectations.core.expectation_suite.ExpectationSuite")
def test_set_suite_exception_handling(
    mock_expectation_suite, mock_get_context, mock_config
):
    # Mocks
    mock_new_suite = MagicMock()
    mock_expectation_suite.side_effect = ValueError(
        "name must be provided as a non-empty string"
    )
    mock_context = mock_get_context.return_value
    mock_context.suites.get.side_effect = Exception("Suite retrieval failed")
    mock_context.suites.add.return_value = mock_new_suite

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context

    # Throw error
    with pytest.raises(ValueError, match="name must be provided as a non-empty string"):
        result.set_suite(mock_config.SUITE_NAME)

    # Asserts
    mock_context.suites.get.assert_called_once_with(mock_config.SUITE_NAME)
    mock_context.suites.add.assert_not_called()


@patch("great_expectations.core.validation_definition.ValidationDefinition")
def test_create_validation_definition(
    mock_validation_definition, mock_get_context, mock_config
):
    # Mocks
    mock_context = mock_get_context.return_value

    mock_validation_instance = MagicMock()
    mock_validation_definition.return_value = mock_validation_instance

    mock_context.validation_definitions = MagicMock()
    mock_context.validation_definitions.add_or_update = MagicMock(
        return_value=mock_validation_instance
    )

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.batch_definition = MagicMock()
    result.suite = MagicMock()
    check_result = result.create_validation_definition()

    # Assertions
    mock_validation_definition.assert_called_once_with(
        name="validation definition",
        data=result.batch_definition,
        suite=result.suite,
    )
    mock_context.validation_definitions.add_or_update.assert_called_once_with(
        mock_validation_instance
    )
    assert check_result is mock_validation_instance


@patch("great_expectations.checkpoint.actions.UpdateDataDocsAction")
@patch("great_expectations.checkpoint.checkpoint.Checkpoint")
def test_create_checkpoint(
    mock_checkpoint, mock_update_action, mock_get_context, mock_config
):
    # Mocks
    mock_context = mock_get_context.return_value
    mock_validation_definition = MagicMock()

    mock_action_instance = MagicMock()
    mock_update_action.return_value = mock_action_instance

    mock_checkpoint_instance = MagicMock()
    mock_checkpoint.return_value = mock_checkpoint_instance

    mock_context.checkpoints = MagicMock()
    mock_context.checkpoints.add_or_update = MagicMock(
        return_value=mock_checkpoint_instance
    )

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context

    result_check = result.create_checkpoint(
        mock_validation_definition, mock_config.SITE_NAME
    )

    # Asserts
    mock_update_action.assert_called_once_with(
        name="update_my_site", site_names=[mock_config.SITE_NAME]
    )
    mock_checkpoint.assert_called_once_with(
        name="checkpoint",
        validation_definitions=[mock_validation_definition],
        actions=[mock_action_instance],
        result_format="COMPLETE",
    )
    mock_context.checkpoints.add_or_update.assert_called_once_with(
        mock_checkpoint_instance
    )
    assert result_check is mock_checkpoint_instance


@patch("great_expectations.checkpoint.checkpoint.Checkpoint")
def test_run_checkpoint(
    mock_checkpoint,
    mock_get_context,
    mock_config,
):
    # Mocks
    mock_context = mock_get_context.return_value
    mock_validation_definition = MagicMock()

    mock_checkpoint_instance = MagicMock()
    mock_checkpoint_instance.run.return_value = {"success": True}

    mock_checkpoint.return_value = mock_checkpoint_instance
    mock_context.create_validation_definition = MagicMock(
        return_value=mock_validation_definition
    )
    mock_context.create_checkpoint = MagicMock(return_value=mock_checkpoint_instance)

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.create_validation_definition = MagicMock(
        return_value=mock_validation_definition
    )
    result.create_checkpoint = MagicMock(return_value=mock_checkpoint_instance)

    checkpoint_result = result.run_checkpoint(mock_config.SITE_NAME)

    # Asserts
    result.create_validation_definition.assert_called_once()
    result.create_checkpoint.assert_called_once_with(
        mock_validation_definition, mock_config.SITE_NAME
    )
    assert checkpoint_result == {"success": True}


def test_generate_data_docs(mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_get_context
    result.generate_data_docs(site_name=mock_config.SITE_NAME)

    # Asserts
    mock_context.build_data_docs.assert_called_once_with(
        site_names=mock_config.SITE_NAME
    )


def test_open_report(mock_get_context, mock_config):
    # Mocks
    mock_context = mock_get_context

    # Call function
    result = GreatExpectationsChecker(mock_config.CONTEXT_MODE)
    result.context = mock_context
    result.open_report()

    # Asserts
    mock_context.open_data_docs.assert_called_once()
