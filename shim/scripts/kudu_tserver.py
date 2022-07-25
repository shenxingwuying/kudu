#!/usr/bin/env python
# -*- coding: utf-8 -*-

from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger
from kudu_base import KuduBase

import time


class KuduTserver(KuduBase):
    def __init__(self):
        super(KuduTserver, self).__init__('tserver')

    def start(self, env):
        # 先检查random_data配置
        config = Script.get_config()
        if config['node_group_params']['random_dir'] is None:
            raise Exception('fail to get random dir, exit')
        # 再检查master
        Logger.info('check if master started')
        start_time = time.time()
        while (time.time() - start_time) < 300:
            if self.check_master_started():
                break
            time.sleep(1)
        else:
            raise Exception('wait master start timeout!')
        Logger.info('check master started')
        super(KuduTserver, self).start(env)


if __name__ == "__main__":
    KuduTserver().execute()
