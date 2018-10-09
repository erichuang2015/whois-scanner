#!/usr/bin/env python3
# encoding=utf-8

import os
import re
import csv

import config
from logger import Logger
from utils import FileHelper

log_name = "clean_csv"
log = Logger.init_logger(log_name)
# 去重后的domain
FILTER_DIR = config.FILTER_DIR
# 判定为重复的domain
UNFILTER_DIR = config.UNFILTER_DIR
# 本地待处理csv
pretreatment_csv_path = config.DOWNLOAD_FTP_TO_LOCAL_PATH
clean_file_bak = pretreatment_csv_path + '/clean_csv_record.txt'
merge_file_bak = pretreatment_csv_path + '/merge_domain_record.txt'
check_domain_bak = pretreatment_csv_path + '/check_domain_record.txt'
# 新增处理文件
deal_with_today = []


class CleanCsv2TXT:
    """
    处理csv 文件
    """
    filter_lst = []
    unfilter_lst = []
    FILTER_NUM = 0
    clean_csv_pool = []
    clean_record_read_finish = False

    @classmethod
    def get_csv_domain(cls):
        csv_dir_lst = cls.get_csv_dir_lst()
        for d in csv_dir_lst:
            log.info("正在读取： %s" % d)
            is_cleaned = False
            _d = re.search("\d{8}", d).group()
            is_cleaned = cls.clean_domain_record(_d, is_cleaned)
            if is_cleaned:
                log.warning("文件 【{}】 已经处理过!".format(_d))
                continue
            else:
                deal_with_today.append(_d)
                log.info("文件 【{}】开始处理!".format(_d))
                for lst in cls.get_csv_lst(d):
                    log.info("正在处理文件: 【{}】".format(lst))
                    with open(lst, 'r', encoding='utf-8') as csvfile:
                        reader = csv.reader(csvfile)
                        domain_column = [row[0] for row in reader]
                        try:
                            # 有的是csv文件是空的
                            domain_column.pop(0)
                        except IndexError:
                            log.warning("该文件格式有误：【{}】".format(lst))
                            pass
                    yield domain_column

                cls.domain_write_to_txt(
                    cls.filter_lst, dir_name=FILTER_DIR.format(_d, _d))
                cls.domain_write_to_txt(
                    cls.unfilter_lst, dir_name=UNFILTER_DIR.format(_d, _d))
                log.info("重复domain数量为: {}".format(cls.FILTER_NUM))
                log.info("过滤后domain数量为: {}".format(len(cls.unfilter_lst)))
                log.info("文件{}去重完成!\n".format(_d))
                cls.FILTER_NUM = 0
                cls.filter_lst.clear()
                cls.unfilter_lst.clear()
        log.info("本次domain过滤完成!\n\n")

    @classmethod
    def get_csv_lst(cls, file_path: str) -> list:
        """
        :param file_path:   未被处理过的文件夹
        :return:   csv_list ---> list
        """
        csv_lst = FileHelper.get_csv_list(file_path)
        return csv_lst

    @classmethod
    def get_csv_dir_lst(cls) -> list:
        """
        获取csv路径下目录
        """
        csv_res = []
        csv_dirs = pretreatment_csv_path
        for item in os.listdir(csv_dirs):
            if '.' in item:
                continue
            elif not re.search('\d{8}', item):
                continue
            else:
                full_path = os.path.join(csv_dirs, item)
                csv_res.append(full_path)
        return csv_res

    @classmethod
    def clean_domain_record(cls, _dir, is_cleaned):
        """
        去重记录
        对去重的文件记录， 判断后仅去重文件中没做记录的
        """
        if os.path.exists(clean_file_bak):
            if cls.clean_record_read_finish:
                pass
            else:
                with open(clean_file_bak, 'r', encoding='utf-8') as reader:
                    for i in reader.readlines():
                        cls.clean_csv_pool.append(i.strip())
                cls.clean_record_read_finish = True
        else:
            with open(clean_file_bak, 'w', encoding='utf-8') as f:
                f.write('')

        with open(clean_file_bak, 'a+', encoding='utf-8') as writer:
            if _dir in cls.clean_csv_pool:
                is_cleaned = True
                return is_cleaned
            else:
                writer.write(_dir + '\n')
                return is_cleaned

    @classmethod
    def domain_filter(cls):
        domain_lst_gener = cls.get_csv_domain()
        for domain_lst in domain_lst_gener:
            for domain in domain_lst:
                if domain not in cls.unfilter_lst:
                    cls.unfilter_lst.append(domain)
                else:
                    cls.FILTER_NUM += 1
                    cls.filter_lst.append(domain)

    @classmethod
    def domain_write_to_txt(cls, domains, dir_name):
        abspath = FileHelper.make_dir(dir_name)
        with open(abspath, 'a', encoding='utf-8') as fp:
            for domain in domains:
                fp.write(domain + '\n')

    @classmethod
    def clean(cls):
        log.info("开始去重!")
        cls.domain_filter()


class DomainManger(object):
    merge_pool = []
    check_pool = []
    merge_record_finish = False
    check_record_finish = False

    @classmethod
    def merge_txt(cls):
        """
        合并历史去重完成的txt文件
        此后的新增domain在此文件中进行判重过滤
        """
        log.info("过滤后的txt文件开始合并.....")
        txt_files_lst = FileHelper.get_txt_list(config.DATA_DIR)
        dir_name = config.MERGE_DIR
        abspath = FileHelper.make_dir(dir_name)
        for lst in txt_files_lst:
            is_merged = False
            _d = re.search("\d{8}", lst).group()
            is_cleaned = cls.merge_record(_d, is_merged)
            if is_cleaned:
                log.warning("文件 【{}】 已经处理过!".format(_d))
                continue
            else:
                log.info("文件 【{}】 开始处理!".format(_d))
                with open(abspath, 'a', encoding='utf-8') as ff:
                    with open(lst, 'r', encoding='utf-8') as fm:
                        reader = fm.readlines()
                        for line in reader:
                            ff.write(line)
        log.info("合并完成！")

    @classmethod
    def check_everyday_with_merge_domain(cls):
        """
        对每天更新的domain数据进行验证去重
        每天的数据与之前合并后的数据校验
        """
        log.info("开始与历史domain校验去重......")
        need_check_domain_txt_lst = FileHelper.get_txt_list(config.DATA_DIR)
        check_lst = []
        check_txt_dir = config.MERGE_DIR
        with open(check_txt_dir, 'r', encoding='utf-8') as fp:
            for line in fp.readlines():
                check_lst.append(line.strip())

        for lst in need_check_domain_txt_lst:
            is_checked = False
            _d = re.search("\d{8}", lst).group()
            is_cleaned = cls.check_record(_d, is_checked)
            if is_cleaned:
                log.warning("文件 【{}】 已经处理过!".format(_d))
                continue
            else:
                log.info("文件 【{}】 开始处理!".format(_d))
                with open(lst, 'r', encoding='utf-8') as old_reader:
                    check_lst_lines = old_reader.readlines()
                with open(lst, 'w', encoding='utf-8') as new_reader:
                    for line in check_lst_lines:
                        if line not in check_lst:
                            new_reader.write(line)
                        else:
                            log.warning("重复domain: %s" % line)

        log.info("验证去重结束!")

    @classmethod
    def merge_record(cls, _dir: str, is_merged: bool):
        """
        合并记录
        对合并的文件记录， 判断后仅合并未做标记的
        """
        if os.path.exists(merge_file_bak):
            if cls.merge_record_finish:
                pass
            else:
                with open(merge_file_bak, 'r', encoding='utf-8') as reader:
                    for i in reader.readlines():
                        cls.merge_pool.append(i.strip())
                cls.merge_record_finish = True
        else:
            with open(merge_file_bak, 'w', encoding='utf-8') as f:
                f.write('')

        with open(merge_file_bak, 'a+', encoding='utf-8') as writer:
            if _dir in cls.merge_pool:
                is_merged = True
                return is_merged
            else:
                writer.write(_dir + '\n')
                return is_merged

    @classmethod
    def check_record(cls, _dir: str, is_checked: bool) -> bool:
        """
        对每次新增的domain数据与本地合并后的数据做过滤
        """
        if os.path.exists(check_domain_bak):
            if cls.check_record_finish:
                pass
            else:
                with open(check_domain_bak, 'r', encoding='utf-8') as reader:
                    for i in reader.readlines():
                        cls.check_pool.append(i.strip())
                cls.check_record_finish = True
        else:
            with open(check_domain_bak, 'w', encoding='utf-8') as f:
                f.write('')

        with open(check_domain_bak, 'a+', encoding='utf-8') as writer:
            if _dir in cls.check_pool:
                is_checked = True
                return is_checked
            else:
                writer.write(_dir + '\n')
                return is_checked

