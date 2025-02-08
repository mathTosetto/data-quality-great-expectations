import sys
import json
import pytest
import logging

from pathlib import Path
from unittest.mock import patch
from datetime import datetime, timezone

from src.utils.my_logger import MyJSONFormatter, LoggerSetup


@pytest.fixture
def mock_logs_dir(tmp_path):
    return tmp_path / "logs"


@pytest.fixture
def mock_config_file(tmp_path):
    """Fixture to create a temporary mock config file with valid JSON."""
    mock_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "verbose",
            }
        },
        "loggers": {
            "my_logger": {
                "handlers": ["console"],
                "level": "DEBUG",
                "propagate": True,
            }
        },
        "formatters": {
            "verbose": {
                "format": "{levelname} {name} {message}",
                "style": "{",
            }
        },
    }

    config_file = tmp_path / "logging_config.json"

    with open(config_file, "w") as f:
        json.dump(mock_config, f)

    return config_file


@pytest.fixture
def log_record():
    """Fixture to create a sample log record"""
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test_path",
        lineno=10,
        msg="Test log message",
        args=(),
        exc_info=None,
    )
    record.created = 1700000000
    return record


def test_create_logs_folder(monkeypatch, mock_logs_dir):
    """Test the create_logs_folder function."""
    # Mocks
    monkeypatch.setattr(
        "src.utils.my_logger.Path",
        lambda path: mock_logs_dir if path == "logs" else Path(path),
    )

    # Asserts
    assert not mock_logs_dir.exists()

    # Call function
    LoggerSetup()

    # Asserts
    assert mock_logs_dir.exists()


@patch("src.utils.my_logger.logging.config.dictConfig")
def test_setup_logging(mock_dict_config, mock_config_file):
    """Test the setup_logging function."""
    # Call function
    loggeer_setup = LoggerSetup(config_path=mock_config_file)

    # Asserts
    with open(mock_config_file) as f:
        mock_config = json.load(f)
    mock_dict_config.assert_called_once_with(config=mock_config)


def test_init_with_custom_fmt_keys():
    """Test if fmt_keys are initialized properly"""
    # Parameters
    fmt_keys = {"msg": "message", "time": "timestamp"}

    # Call function
    formatter = MyJSONFormatter(fmt_keys=fmt_keys)

    # Asserts
    assert formatter.fmt_keys == fmt_keys


def test_init_with_default_fmt_keys():
    """Test if fmt_keys default to an empty dictionary when not provided"""
    # Call function
    formatter = MyJSONFormatter()

    # Asserts
    assert formatter.fmt_keys == {}


def test_prepare_log_dict(log_record):
    """Test if log dictionary is prepared correctly"""
    # Parameters
    fmt_keys = {"msg": "message", "time": "timestamp"}

    # Call function
    formatter = MyJSONFormatter(fmt_keys=fmt_keys)
    log_dict = formatter._prepare_log_dict(log_record)

    # Asserts
    assert "msg" in log_dict
    assert log_dict["msg"] == "Test log message"

    assert "time" in log_dict
    assert (
        log_dict["time"]
        == datetime.fromtimestamp(log_record.created, tz=timezone.utc).isoformat()
    )


def test_format(log_record):
    """Test if format method correctly converts log record to JSON"""
    # Parameters
    fmt_keys = {"msg": "message", "time": "timestamp"}

    # Call function
    formatter = MyJSONFormatter(fmt_keys=fmt_keys)

    formatted_log = formatter.format(log_record)
    log_json = json.loads(formatted_log)

    # Asserts
    assert log_json["msg"] == "Test log message"
    assert (
        log_json["time"]
        == datetime.fromtimestamp(log_record.created, tz=timezone.utc).isoformat()
    )
    assert "timestamp" not in log_json


def test_format_with_exception():
    """Test if exception information is included when present"""
    try:
        raise ValueError("Test exception")
    except ValueError:
        exc_info = sys.exc_info()

        log_record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test_path",
            lineno=20,
            msg="Error log message",
            args=(),
            exc_info=exc_info,
        )

        # Call function
        formatter = MyJSONFormatter()
        formatted_log = formatter.format(log_record)
        log_json = json.loads(formatted_log)

        # Asserts
        assert "exc_info" in log_json
        assert "ValueError" in log_json["exc_info"]
        assert "Test exception" in log_json["exc_info"]


def test_format_with_stack_info(log_record):
    """Test if stack information is included when present"""
    # Mocks
    log_record.stack_info = "Test stack trace"

    # Call function
    formatter = MyJSONFormatter()
    formatted_log = formatter.format(log_record)
    log_json = json.loads(formatted_log)

    # Assserts
    assert "stack_info" in log_json
    assert log_json["stack_info"] == "Test stack trace"
