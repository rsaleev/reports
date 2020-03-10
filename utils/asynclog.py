from aiologger import Logger
from aiologger.loggers.json import JsonLogger, ExtendedJsonFormatter
from aiologger.levels import LogLevel
from aiologger.handlers.files import AsyncTimedRotatingFileHandler, RolloverInterval, BaseAsyncRotatingFileHandler
from aiologger.formatters.base import Formatter
from aiologger.formatters.json import JsonFormatter, LOGGED_AT_FIELDNAME, LOG_LEVEL_FIELDNAME, LINE_NUMBER_FIELDNAME, FILE_PATH_FIELDNAME, FUNCTION_NAME_FIELDNAME
import asyncio
from aiologger.handlers.files import AsyncFileHandler
from tempfile import NamedTemporaryFile


class AsyncLogger:

    @classmethod
    async def getlogger(cls, log_file):
        logger = JsonLogger(serializer_kwargs={'ensure_ascii': False, 'indent': 4})
        logger.name = __name__
        handler = AsyncTimedRotatingFileHandler(
            filename=log_file,
            when=RolloverInterval.DAYS,
            interval=1,
            backup_count=31,
            encoding='utf-8')
        handler.formatter = ExtendedJsonFormatter(default_msg_fieldname=LOGGED_AT_FIELDNAME, exclude_fields=[LINE_NUMBER_FIELDNAME, FILE_PATH_FIELDNAME, FUNCTION_NAME_FIELDNAME])
        logger.add_handler(handler)
        return logger
