#!/bin/env python3
# -*- coding: UTF-8 -*-

"""
sokuadmin是所有kudu的脚本工具的集合
这个脚本用来指向了真正使用的脚本
"""
import sys
import os
from bridge_console.admin import main

current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(current_dir)

sys.exit(main('soku', current_dir))