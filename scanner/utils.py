#!/usr/bin/env python3
# encoding=utf-8

import os
import hashlib
import tldextract


class FileHelper:

    @classmethod
    def get_csv_list(cls, csv_file_path: str, recursion=True) -> list:
        """
        默认递归获取路径下所以.csv文件
        :param csv_file_path:     根目录
        :param recursion:     是否递归
        :return:    list  -->   csv_file
        """
        res = []
        for item in os.listdir(csv_file_path):
            full_path = os.path.join(csv_file_path, item)
            if os.path.isfile(full_path):
                res.append(full_path)
            if recursion and os.path.isdir(full_path):
                res.extend(cls.get_csv_list(full_path, recursion=recursion))
        return res

    @classmethod
    def get_txt_list(cls, txt_file_path, recursion=True) -> list:
        """
        遍历路径下txt文件
        """
        res = []
        for item in os.listdir(txt_file_path):
            full_path = os.path.join(txt_file_path, item)
            if os.path.isfile(full_path) and item.startswith('un'):
                res.append(full_path)
            if recursion and os.path.isdir(full_path) and item.isdigit():
                res.extend(cls.get_txt_list(full_path, recursion=recursion))
        return res

    @staticmethod
    def make_dir(file_path: str) -> str:

        abspath = os.path.abspath(file_path)
        pardir = abspath.rsplit(os.sep, 1)[0]
        if not os.path.exists(pardir):
            os.makedirs(pardir, exist_ok=True)
        return abspath


def primary_domain(host: str) -> str:
    tld_result = tldextract.extract(host)
    domain = host
    if tld_result.domain:
        domain = tld_result.domain + '.' + tld_result.suffix
    return domain


def domain2rowkey(domain: str) -> str:
    return hashlib.md5(primary_domain(domain).encode('utf-8')).hexdigest() + '-' + domain[::-1]


