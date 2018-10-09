#!/usr/bin/env python3
# encoding=utf-8

import time
import contextlib

import happybase

import config
from logger import Logger

HBASE_HOST = config.HBASE_HOST
HBASE_PORT = config.HBASE_PORT
HBASE_TABLE_PREFIX = config.HBASE_TABLE_PREFIX
HAPPYBASE_POOL_SIZE = config.HAPPYBASE_POOL_SIZE
HAPPYBASE_RECONNECT_TIMES = config.HAPPYBASE_RECONNECT_TIMES
HAPPYBASE_RECONNECT_WAIT_SECONDS = config.HAPPYBASE_RECONNECT_WAIT_SECONDS

name = "hbasehelper"


class HbaseWrapper:
    """
    Hbase
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str=HBASE_HOST, port: int=HBASE_PORT, table_prefix: str=HBASE_TABLE_PREFIX, timeout=60):
        self.hbase_host = host
        self.hbase_port = port
        self.timeout = timeout
        self.table_prefix = table_prefix
        self.connection_pool = happybase.ConnectionPool(
                                                        HAPPYBASE_POOL_SIZE,
                                                        host=self.hbase_host,
                                                        port=self.hbase_port,
                                                        table_prefix=self.table_prefix,
                                                        table_prefix_separator=':',
                                                        transport='framed'
                                                        )
        self.reconnect_count = 0
        self.log = Logger.init_logger(name)
        self.log.info('init hbase: {}'.format(host))

    @contextlib.contextmanager
    def connect(self):

        try:
            self.connection_pool.connection(self.timeout)
        except Exception as e:
            while self.reconnect_count < HAPPYBASE_RECONNECT_TIMES:
                self.reconnect_count += 1
                self.log.error(
                    'connect hbase:  {} error,start reconnecting,has retried {} times'.
                    format(self.hbase_host, self.reconnect_count))
                self.connect()
                time.sleep(HAPPYBASE_RECONNECT_WAIT_SECONDS)
            self.log.error(
                'reconnect times have been max,please check the error')
            raise Exception(str(e))

    def get_table(self, table_name: str=config.HBASE_TABLE) -> str:
        """
        Get Table
        :param table_name:
        :return: target_table
        """
        try:
            with self.connection_pool.connection(self.timeout) as connection:
                target_table = connection.table(table_name)
            self.log.info(
                'target table: {} get successfully.'.format(table_name))
        except Exception as e:
            self.log.error('target table: {} get fail.'.format(
                table_name, str(e)))
            raise Exception(str(e))
        return target_table
