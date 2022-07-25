#!/bin/env python3
# -*- coding: UTF-8 -*-
import os
import sys
import time
sys.path.append(os.path.join(os.environ['MOTHERSHIP_HOME'], 'shim_libs'))
from base_custom_callback import BaseCustomCallback

from hyperion_utils.shell_utils import ShellClient


class ModuleCustomCallback(BaseCustomCallback):
    """
    module 级别自定义回调，仅在 module 部署的其一机器执行一次
    """
    def __init__(self, *args):
        super().__init__(*args)
        self.module_name = self.params['module_params']['module_name']
        kudu_master_hosts = self.params['cluster_node_info']['kudu_master']['nodes']
        self.master_addrs = ','.join(['%s' % (h) for h in kudu_master_hosts])

    def upgrade_prepare_ready_check(self, **kwargs):
        """ 升级前 prepare 是否就绪检查
        Args:
            params:
            **kwargs:

        Returns:

        """
        self.service_check()

    def service_check(self, **kwargs):
        """ 服务检查

        Args:
            params:
            **kwargs:

        Returns:

        """
        ksck_cmd = "kudu cluster ksck %s" % self.master_addrs
        """重试超时时间为10分钟"""
        end_time = time.time() + 600
        while (time.time() < end_time):
            try:
                ksck_ret = ShellClient.call(ksck_cmd)
                if (ksck_ret != 0):
                    time.sleep(10)
                    continue
                break
            except Exception:
                time.sleep(10)
        else:
            raise Exception(f'service_check kudu ksck 异常 [ret = {ksck_ret}, cmd: {ksck_cmd}]')


if __name__ == "__main__":
    ModuleCustomCallback(*sys.argv[1:]).execute()
