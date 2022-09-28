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
        # 最后启动collector
        super(KuduCollector, self).start(env)


if __name__ == "__main__":
    KuduCollector().execute()
