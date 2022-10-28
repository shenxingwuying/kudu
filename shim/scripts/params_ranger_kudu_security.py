#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 用于生成ranger-kudu-security.xml
import os
import params
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()

ranger_kudu_security_config = {k: v for k, v in config['configurations']['ranger_kudu_security'].items()}

if params.enable_ranger:
    ranger_kudu_security_config['ranger.plugin.kudu.policy.cache.dir'] = params.kudu_conf_dir
    ranger_kudu_security_config['ranger.plugin.kudu.policy.rest.url'] = params.ranger_address
    ranger_kudu_security_config['ranger.plugin.kudu.service.name'] = params.ranger_service_name
