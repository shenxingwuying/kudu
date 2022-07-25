#!/usr/bin/env python
# -*- coding: utf-8 -*-
from kudu_base import KuduBase
from resource_management.core.resources.system import Execute
from resource_management.core.logger import Logger

import os
import time


class KuduCollector(KuduBase):
    def __init__(self):
        super(KuduCollector, self).__init__('collector')

    def make_links(self):
        """建立对应的软链"""
        import params
        # tool
        tool_src = os.path.join(params.collector_binary_home, 'bin/kudu')
        tool_dst = os.path.join(params.collector_binary_home, 'sbin/kudu')
        if os.path.islink(tool_dst):
            cmd = "rm -f '%s'" % tool_dst
            Execute(cmd, user=params.kudu_user)
        cmd = "ln -s '%s' '%s'" % (tool_src, tool_dst)
        Execute(cmd, user=params.kudu_user)

    def start(self, env):
        # 先检查master
        Logger.info('check if master started')
        start_time = time.time()
        while (time.time() - start_time) < 300:
            if self.check_master_started():
                break
            time.sleep(1)
        else:
            raise Exception('wait master start timeout!')
        Logger.info('check master started')
        # 再创建软链供collector使用
        self.make_links()
        # 最后启动collector
        super(KuduCollector, self).start(env)


if __name__ == "__main__":
    KuduCollector().execute()
