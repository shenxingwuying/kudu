#!/usr/bin/env python
# -*- coding: utf-8 -*-
from kudu_base import KuduBase
from resource_management.core.resources.system import Execute
from resource_management.core.logger import Logger
from resource_management.libraries.functions import format

import os
import time


class KuduCollector(KuduBase):
    def __init__(self):
        super(KuduCollector, self).__init__('collector')

    def start(self, env):
        # 检查master
        Logger.info('check if master started')
        start_time = time.time()
        while (time.time() - start_time) < 300:
            if self.check_master_started():
                break
            time.sleep(1)
        else:
            raise Exception('wait master start timeout!')
        # 再检查tserverer,防止集群环境上collector启动时创建表失败
        Logger.info('check if tserver started')
        start_time = time.time()
        while (time.time() - start_time) < 300:
            if self.check_tserver_started():
                break
            time.sleep(1)
        else:
            raise Exception('wait tserver start timeout!')
        Logger.info('check master started')
        super(KuduCollector, self).start(env)


if __name__ == "__main__":
    KuduCollector().execute()
