import json
import logging
import logging.config

import datetime as dt

from pathlib import Path
from typing import override


class MyJSONFormatter(logging.Formatter):
    """Custom logging formatter that outputs logs in JSON format."""

    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        """
        Initializes the JSON formatter with optional keys to customize log fields.

        Args:
            fmt_keys (dict[str, str] | None, optional): A dictionary where keys represent the
            log field names and values represent the corresponding record attributes to use.
            Defaults to None, which means no customization.
        """
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        """
        Formats the log record into a JSON string.

        Args:
            record (logging.LogRecord): The log record to format.

        Returns:
            str: The formatted log as a JSON string.
        """
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
        """
        Prepares a dictionary with log information for JSON formatting.

        Args:
            record (logging.LogRecord): The log record to prepare the dictionary for.

        Returns:
            dict: A dictionary containing log details to be formatted into JSON.
        """
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = record.stack_info

        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        return message


class LoggerSetup:
    """Sets up logging configuration for the application, using a JSON configuration file."""

    def __init__(self, config_path: str = "logs/logging_json/logging_config.json"):
        """
        Initializes the logger setup with the specified configuration file path.

        Args:
            config_path (str, optional): Path to the JSON configuration file for logging setup.
            Defaults to "logs/logging_json/logging_config.json".
        """
        self.config_path = Path(config_path)
        self._create_logs_folder()
        self._setup_logging()

    def _create_logs_folder(self):
        """Creates the logs folder if it does not exist."""
        Path("logs").mkdir(exist_ok=True)

    def _setup_logging(self):
        """Sets up logging configuration from the specified JSON configuration file."""
        with open(self.config_path) as cfg:
            config = json.load(cfg)
        logging.config.dictConfig(config=config)
