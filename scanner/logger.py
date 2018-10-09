#!/usr/bin/env python3
# encoding=utf-8

import os
import logging
from logging import handlers
from distutils.dir_util import mkpath

from config import LOG_PATH


UTF_8 = 'utf-8'
NAME = 'clean'
MAX_BACK_FILE_NUM = 10
MAX_BACK_FILE_SIZE = 256 * 1024 * 1024


class Logger:

    @staticmethod
    def get_logger_name(log_name: str):
        return NAME + '_' + log_name

    @staticmethod
    def get_logger_file_name(logger_name: str):
        return logger_name + '.log'

    @staticmethod
    def get_or_create_log_path():
        storage_path = LOG_PATH
        if not os.path.exists(storage_path):
            mkpath(storage_path)

        return storage_path

    @staticmethod
    def get_logger_format():
        fmt = '[%(asctime)s]'
        fmt += '-[%(levelname)s]'
        fmt += '-[%(process)d]'
        fmt += '-[%(filename)s:%(lineno)s]'
        fmt += ' # %(message)s'
        return fmt

    @classmethod
    def add_rotating_file_handler(cls, logger, logger_name: str, formatter: get_logger_format):
        file_name = cls.get_or_create_log_path() + cls.get_logger_file_name(logger_name)
        handler = handlers.RotatingFileHandler(
            file_name,
            maxBytes=MAX_BACK_FILE_SIZE,
            backupCount=MAX_BACK_FILE_NUM,
            encoding=UTF_8
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    @staticmethod
    def add_stream_handler(logger, formatter):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    @classmethod
    def init_logger(cls, log_name, log_level=logging.INFO):
        logger_name = cls.get_logger_name(log_name)
        logger = logging.getLogger(logger_name)
        formatter = logging.Formatter(cls.get_logger_format())
        cls.add_rotating_file_handler(logger, logger_name, formatter)
        cls.add_stream_handler(logger, formatter)
        logger.setLevel(log_level)
        return logger

