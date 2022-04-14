#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys
import time

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from upgrader.step.dir_step import DirStep
from hyperion_client.config_manager import ConfigManager
from hyperion_utils import shell_utils


class CollectorUpgraderStep(DirStep):
    def __init__(self):
        super().__init__()

    def get_master_addr(self):
        master_addrs = ""
        """首先通过soku/kudu获取地址，如果出错则继续，不报错则返回"""
        try:
            master_addrs = ConfigManager().get_client_conf("soku", "kudu")['master_address']
        except Exception as e:
            self.logger.warning("获取master地址失败，可能是老版本接口不支持: %s " % e)
        if master_addrs != "":
            return master_addrs
        """最后通过sp/kudu获取地址，如果出错则直接返回空，不报错返回master_addrs"""
        try:
            master_addrs = ConfigManager().get_client_conf("sp", "kudu")['master_address']
        except Exception as e:
            self.logger.warning("获取master地址失败: %s " % e)
            return ""
        if master_addrs == "":
            raise Exception("获取master地址失败，请检查kudu服务是否正常！")
        return master_addrs

    def start_module(self):
        if self.my_host != self.first_host:
            return
        """判断Kudu服务是否正常, Collector启动需要获取TServer地址"""
        master_addrs = self.get_master_addr()
        command = "kudu cluster ksck %s" % master_addrs
        result = shell_utils.run_cmd(command, self.logger.info)
        if result['ret'] != 0:
            now = time.time()
            """重试超时时间为10分钟"""
            end_time = now + 600
            kudu_ok_flag = False
            while now < end_time:
                """每隔10s尝试一次"""
                time.sleep(10)
                result = shell_utils.run_cmd(command, self.logger.info)
                if result['ret'] == 0:
                    kudu_ok_flag = True
                    break
                now = time.time()
            if not kudu_ok_flag:
                raise Exception("Kudu集群ksck状态不OK，已重试10分钟，collector启动失败。请手动检测Kudu集群ksck状态为OK后，再继续!")
        cmd = 'spadmin start -p soku -m collector'
        shell_utils.check_call(cmd, self.logger.debug)
