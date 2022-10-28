#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 用于生成core-site.xml
import os
import params
from resource_management.libraries.script.script import Script


# server configurations
config = Script.get_config()

core_site_config = {k: v for k, v in config['configurations']['core-site'].items()}
