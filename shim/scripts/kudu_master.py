#!/usr/bin/env python
# -*- coding: utf-8 -*-

from resource_management.libraries.script.script import Script
from kudu_base import KuduBase


class KuduMaster(KuduBase):
    def __init__(self):
        super(KuduMaster, self).__init__('master')

    def start(self, env):
        # 先检查meta_data配置
        config = Script.get_config()
        if config['node_group_params']['meta_dir'] is None:
            raise Exception('fail to get meta dir, exit')
        super(KuduMaster, self).start(env)


if __name__ == "__main__":
    KuduMaster().execute()
