#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 用于生成ranger-kudu-policymgr-ssl.xml
# 暂时没有需要配置的属性，但需要这个配置文件存在

import params
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()

ranger_kudu_policymgr_config = {k: v for k, v in config['configurations']['ranger_kudu_policymgr'].items()}
