#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from upgrader.step.dir_step import DirStep
import utils.shell_wrapper
from subprocess import getoutput


class SokuToolUpgraderStep(DirStep):
    def __init__(self):
        super().__init__()
        self.package_dir = self.root_path
        self.logger.info('soku_tool update, package_dir is %s' % self.package_dir)
        self.package_tar = ''
        for file in os.listdir(self.package_dir):
            if file.startswith('soku_tool-') and file.endswith('.tar'):
                self.package_tar = file
                break
        self.update_tar_full_path = os.path.join(self.package_dir, self.package_tar)

    def update(self):
        # 更新文件和版本号
        self.update_files()
        self.change_image_tag_to_current()
        soku_home = os.environ['SENSORS_SOKU_HOME']
        # 先将对应版本的soku_tool替换
        cmd = "tar xvf '%s' -C '%s'" % (self.update_tar_full_path, soku_home)
        utils.shell_wrapper.check_call(cmd, self.logger.debug)
        # 更新系统目录下kudu工具的链接位置到soku_tool目录下
        dir_kudu = os.path.join("/usr/bin", "kudu")
        if os.path.islink(dir_kudu) or os.path.isfile(dir_kudu):
            cmd = "sudo rm -f '%s'" % dir_kudu
            utils.shell_wrapper.check_call(cmd, self.logger.debug)
        src_kudu = os.path.join(soku_home, "soku_tool/kudu")
        cmd = "sudo ln -s '%s' '%s'" % (src_kudu, dir_kudu)
        utils.shell_wrapper.check_call(cmd, self.logger.debug)
        dir_sp_kudu = os.path.join("/usr/bin", "sp_kudu")
        if os.path.islink(dir_sp_kudu) or os.path.isfile(dir_sp_kudu):
            cmd = "sudo rm -f '%s'" % dir_sp_kudu
            utils.shell_wrapper.check_call(cmd, self.logger.debug)
        src_sp_kudu = os.path.join(soku_home, "soku_tool/sp_kudu")
        cmd = "sudo ln -s '%s' '%s'" % (src_sp_kudu, dir_sp_kudu)
        utils.shell_wrapper.check_call(cmd, self.logger.debug)
        # CDH环境下kudu会链接到该目录下，且直接替换的方式并不能使/usr/bin/kudu的优先级更高
        # 保险起见，直接将这里的kudu链接到soku_tool目录下,升级时直接替换源soku_tool目录即可
        cdh_home = getoutput("""cat ~/.bashrc | grep PATH | grep CDH | sed "s/[=:']/ /g" | awk '{print $3}'""")
        kudu_tool_path = os.path.join(cdh_home, "../../CDH/bin/kudu")
        if os.path.islink(kudu_tool_path) or os.path.isfile(kudu_tool_path):
            cmd = "sudo rm -f '%s'" % kudu_tool_path
            utils.shell_wrapper.check_call(cmd, self.logger.debug)
            cmd = "sudo ln -s '%s' '%s'" % (src_kudu, kudu_tool_path)
            utils.shell_wrapper.check_call(cmd, self.logger.debug)
        kudu_sensors_tool_path = os.path.join(cdh_home, "../../KUDU_SENSORS_DATA/bin/kudu")
        if os.path.islink(kudu_sensors_tool_path) or os.path.isfile(kudu_sensors_tool_path):
            cmd = "sudo rm -f '%s'" % kudu_sensors_tool_path
            utils.shell_wrapper.check_call(cmd, self.logger.debug)
            cmd = "sudo ln -s '%s' '%s'" % (src_kudu, kudu_sensors_tool_path)
            utils.shell_wrapper.check_call(cmd, self.logger.debug)

    def rollback(self):
        # 1.把backup目录下的文件全部拷贝回去
        self.restore_files()
        self.rollback_image_tag()

    def check(self):
        # 适配 upgrader 通用流程，不需要校验
        return True
