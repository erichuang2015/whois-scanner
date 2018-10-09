#!/usr/bin/env python3
# encoding=utf-8

import re
import os

import config
from logger import Logger
from utils import FileHelper
from utils import domain2rowkey
from hbasehelper import HbaseWrapper

log_name = "scan_whois"
log = Logger.init_logger(log_name)
# 扫描后的domain
NOT_IN_HBASE_DOMAIN = config.NOT_IN_HBASE_DOMAIN
hbase_record_bak = config.DOWNLOAD_FTP_TO_LOCAL_PATH + '/hbase_check_record.txt'


class ScanRemoteWhoIs:
    """
    从Hbase 中查询本地whois数据比对
    """
    scan_domain_pool = []
    merge_record_finish = False
    hbase_cursor = HbaseWrapper()
    table = hbase_cursor.get_table()

    @classmethod
    def scanner(cls):
        """
        比较查询domain
        """
        log.info("开始扫描whois......")
        need_scan_domain_txt_lst = FileHelper.get_txt_list(config.DATA_DIR)
        not_in_hbase_domains = []
        for lst in need_scan_domain_txt_lst:
            is_scanned = False
            _d = re.search("\d{8}", lst).group()
            is_scanned = cls.scan_record(_d, is_scanned)
            if is_scanned:
                log.warning("文件 【{}】 已经处理过!".format(_d))
                continue
            else:
                log.info("文件 【{}】 开始处理!".format(_d))
                try:
                    with open(lst, 'r', encoding='utf-8') as old_reader:
                        scan_lst_domain_lines = old_reader.readlines(
                        )  # 取出的本地过滤后的domains
                    for domain in scan_lst_domain_lines:
                        log.info("正在查询：【{}】".format(domain))
                        dom_key = domain2rowkey(domain.strip())
                        item = cls.table.row(dom_key)
                        if item:
                            continue
                        else:
                            not_in_hbase_domains.append(domain)

                    cls.scan_domain_write_txt(
                        not_in_hbase_domains,
                        dir_name=NOT_IN_HBASE_DOMAIN.format(_d, _d))
                    log.info("扫描hbase得到domain个数为: {}".format(
                        len(not_in_hbase_domains)))
                    log.info("文件【{}】扫描完成!\n".format(_d))
                    not_in_hbase_domains.clear()

                except Exception as e:
                    log.error(str(e))

        log.info("本次hbase扫描结束!")

    @classmethod
    def scan_domain_write_txt(cls, domains: list, dir_name: str):
        """

        :param domains:      扫描hbase后的domains
        :param dir_name:     每次扫描后的结果存放地址
        """
        abspath = FileHelper.make_dir(dir_name)
        with open(abspath, 'a', encoding='utf-8') as fp:
            for domain in domains:
                fp.write(domain)

    @classmethod
    def scan_record(cls, _dir: str, is_scanned: bool) -> bool:
        """
        扫描比较记录
        """
        if os.path.exists(hbase_record_bak):
            if cls.merge_record_finish:
                pass
            else:
                with open(hbase_record_bak, 'r', encoding='utf-8') as reader:
                    for i in reader.readlines():
                        cls.scan_domain_pool.append(i.strip())
                cls.merge_record_finish = True
        else:
            with open(hbase_record_bak, 'w', encoding='utf-8') as f:
                f.write('')

        with open(hbase_record_bak, 'a+', encoding='utf-8') as writer:
            if _dir in cls.scan_domain_pool:
                is_scanned = True
                return is_scanned
            else:
                writer.write(_dir + '\n')
                return is_scanned
