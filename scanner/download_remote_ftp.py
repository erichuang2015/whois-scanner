#!/usr/bin/env python3
# encoding=utf-8

import os
import re
from ftplib import FTP, FTP_TLS

import config
from logger import Logger

hostname = config.HOSTNAME
username = config.USERNAME
password = config.PASSWORD
log_name = "download_ftp"
log = Logger.init_logger(log_name)
local_path = config.DOWNLOAD_FTP_TO_LOCAL_PATH
download_file_bak = local_path + "/download_ftp_record.txt"
# 只记录FTP服务器的根文件夹
download_file_bak_count = 0


class FTPSync:
    def __init__(self,
                 hostname=hostname,
                 username=username,
                 password=password,
                 base_url='/DailyWhoisData/',
                 secure=False,
                 passive=True):

        try:
            if secure:
                self.ftp_client = FTP_TLS(
                    host=hostname, user=username, passwd=password)
                self.ftp_client.prot_p()

            else:
                self.ftp_client = FTP(
                    host=hostname, user=username, passwd=password)
                self.ftp_client.set_pasv(passive)
        except Exception as e:
            log.error(str(e))

        self.expand = True
        self.download_record_pool = []
        self.ftp_record_read_finish = False
        self.base_url = base_url.rstrip('/')
        self._init_local_path()

    def _init_local_path(self):
        """创建本地文件保存路径"""
        if not os.path.exists(local_path):
            log.info("初始化本地文件路径")
            os.mkdir(local_path)
        else:
            log.warning("文件目录已存在！")

        os.chdir(local_path)

    def get_dirs_files(self):
        """
        获取目录与文件
        """
        dir_res = []
        self.ftp_client.dir('.', dir_res.append)
        files = [f.split(None, 8)[-1] for f in dir_res if f.startswith('-')]
        dirs = [f.split(None, 8)[-1] for f in dir_res if f.startswith('d')]
        return files, dirs

    def download_record(self, _dir: str, is_download: bool):
        """
        下载记录
        对下载的文件记录， 判断后仅下载文件中没做记录的
        """
        if os.path.exists(download_file_bak):
            if self.ftp_record_read_finish:
                pass
            else:
                with open(download_file_bak, 'r', encoding='utf-8') as reader:
                    for i in reader.readlines():
                        self.download_record_pool.append(i.strip())
                self.ftp_record_read_finish = True
        else:
            with open(download_file_bak, 'w') as f:
                f.write('')

        with open(download_file_bak, 'a+', encoding='utf-8') as writer:
            if _dir in self.download_record_pool:
                is_download = True
                return is_download
            else:
                writer.write(_dir + '\n')
                return is_download

    def recursive_local_dir(self, next_dir: str):
        self.ftp_client.cwd(next_dir)
        if self.expand:
            try:
                os.mkdir(local_path)
            except OSError:
                pass
            os.chdir(local_path)
            self.expand = False
        else:
            try:
                os.mkdir(next_dir)
            except OSError:
                pass
            os.chdir(next_dir)

        ftp_curr_dir = self.ftp_client.pwd()
        local_curr_dir = os.getcwd()
        files, dirs = self.get_dirs_files()

        for f in files:
            try:
                log.debug('downloading :', os.path.abspath(f))
                with open(f, 'wb') as writer:
                    self.ftp_client.retrbinary('RETR %s' % f, writer.write)
            except Exception as e:
                log.error(str(e))

        for d in dirs:
            is_download = False
            if re.match('\d{8}', d):
                log.info("正在下载【{}】".format(d))
                is_download = self.download_record(d, is_download)
            os.chdir(local_curr_dir)
            if is_download:
                log.warning("文件 [{}] 已经下载过!!".format(d))
                continue
            else:
                self.ftp_client.cwd(ftp_curr_dir)
                self.recursive_local_dir(d)  # 递归切换本地与ftp服务器路径

    def init_ftp(self):
        self.recursive_local_dir(self.base_url)
        log.info("本次文件更新完成!")





