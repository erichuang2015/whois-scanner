#!/usr/bin/env python3
# encoding=utf-8

from os.path import expanduser

# HBASE
HBASE_HOST = '127.0.0.1'
HBASE_PORT = 9200
HBASE_TABLE = 'domain'
HBASE_TABLE_PREFIX = 'i'
HAPPYBASE_POOL_SIZE = 2
# 重连次数
HAPPYBASE_RECONNECT_TIMES = 100
# 睡眠时间
HAPPYBASE_RECONNECT_WAIT_SECONDS = 5


# FTP
HOSTNAME = ''
USERNAME = ''
PASSWORD = ''

# FTP 服务器下载的数据本地保存地址
DOWNLOAD_FTP_TO_LOCAL_PATH = expanduser("~/whois-scanner/DailyWhoisData")
# 处理后的文件保存地址
DATA_DIR = DOWNLOAD_FTP_TO_LOCAL_PATH + "/data"
# 去重后的domain
FILTER_DIR = DATA_DIR + "/{}/filter_{}.txt"
# 判定为重复的domain
UNFILTER_DIR = DATA_DIR + "/{}/unfilter_{}.txt"
# 扫描不在hbase中的domain
NOT_IN_HBASE_DOMAIN = DATA_DIR + "/{}/not_in_hbase{}.txt"
# 合并后的txt文件
MERGE_DIR = DATA_DIR + "/merge_unfilter.txt"
# 日志存放地址
LOG_PATH = DOWNLOAD_FTP_TO_LOCAL_PATH + '/log/'
