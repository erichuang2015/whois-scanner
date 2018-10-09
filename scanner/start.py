#!/usr/bin/env python3
# encoding=utf-8

from threading import Timer

from clean_csv import CleanCsv2TXT
from scan_whois import ScanRemoteWhoIs
from download_remote_ftp import FTPSync


# 查询时间间隔
QUERY_TIME_INTERVAL = 60 * 60 * 24

def init():
    """
    调用ftp增量下载
    本地domain过滤
    """
    ftp = FTPSync()
    ftp.init_ftp()  # 启动ftp下载
    CleanCsv2TXT.clean()  # 启动domain过滤
    ScanRemoteWhoIs.scanner()   # 启动hbase扫描


def loop_query():

    global count
    count += 1
    print(count)
    init()
    if count < float("inf"):
        t = Timer(QUERY_TIME_INTERVAL, loop_query)
        t.start()



if __name__ == '__main__':
    count = 0
    loop_query()

