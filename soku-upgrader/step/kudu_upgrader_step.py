#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys

sys.path.append(os.environ['SENSORS_PLATFORM_HOME'])
from construction_blueprint.installer_constants import HadoopDistributionType

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from upgrader.step.dir_step import DirStep
from upgrader.step.base_upgrader_step import BaseUpgraderStep
from hyperion_client.deploy_info import DeployInfo
import utils.shell_wrapper

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'tools'))
from mothership_upgrader_tool import MothershipUpgraderTool
from cdh_upgrader_tool import CdhUpgraderTool

sys.path.append(os.path.join(os.path.dirname(__file__), '../..', 'construction_blueprint', 'tools'))
from cdh_config_tool import KuduConfigTool
from mothership_config_tool import MothershipKuduConfigTool
from config_common import MOTHERSHIP_UPDATE_CONFIG, CDH_UPDATE_CONFIG


class KuduUpgraderStep(DirStep):
    def __init__(self):
        super().__init__()
        self.package_dir = self.root_path
        self.logger.info('kudu update, package_dir is %s' % self.package_dir)

    def update(self):
        # 更新文件和版本号
        self.update_files()
        self.change_image_tag_to_current()
        if DeployInfo().get_hadoop_distribution() == HadoopDistributionType.MOTHERSHIP:
            mothershipUpgraderTool = MothershipUpgraderTool(self.logger, self.package_dir)
            mothershipUpgraderTool.update()
            MothershipKuduConfigTool().do_update(MOTHERSHIP_UPDATE_CONFIG)
        elif DeployInfo().get_hadoop_distribution() == HadoopDistributionType.CLOUDERA:
            # todo parcel 升级是否需要设置强制升级，目前会根据版本号判断是否升级
            cdhUpgraderTool = CdhUpgraderTool(self.logger, self.package_dir)
            cdhUpgraderTool.update()
            KuduConfigTool().do_update(CDH_UPDATE_CONFIG)
        else:
            # 混部场景，暂时不处理
            self.logger.info('hadoop distribution type is mix, no need to update')

    def rollback(self):
        # 1.把backup目录下的文件全部拷贝回去
        self.restore_files()
        self.rollback_image_tag()
        if os.path.isdir(self.backup_dir):
            new_backup_name = BaseUpgraderStep.backup_name(self.backup_dir)
            utils.shell_wrapper.check_call("mv '%s' '%s'" % (self.backup_dir, new_backup_name), self.logger.debug)
            self.logger.info('renamed backup dir from %s to %s' % (self.backup_dir, new_backup_name))
        if DeployInfo().get_hadoop_distribution() == HadoopDistributionType.MOTHERSHIP:
            self.logger.info('soku rollback for mothership')
            mothershipUpgraderTool = MothershipUpgraderTool(self.logger, self.package_dir)
            mothershipUpgraderTool.rollback()
        else:
            self.logger.info('no need rollback for cdh or mix')

    def check(self):
        # 适配 upgrader 通用流程，不需要校验
        return True
