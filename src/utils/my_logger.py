import json
import logging

import datetime as dt

from pathlib import Path
from typing import override


class MyJSONFormatter(logging.Formatter):
    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
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
    def __init__(self, config_path: str = "logs/logging_json/logging_config.json"):
        self.config_path = Path(config_path)
        self._create_logs_folder()
        self._setup_logging()

    def _create_logs_folder(self):
        Path("logs").mkdir(exist_ok=True)

    def _setup_logging(self):
        with open(self.config_path) as cfg:
            config = json.load(cfg)
        logging.config.dictConfig(config=config)
