#!./python/bin/python3
# -*- coding: UTF-8 -*-
import os
import sys

sys.path.append(os.environ['SENSORS_PLATFORM_HOME'])
from construction_blueprint.installer_constants import HadoopDistributionType

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from construction_vehicle.step.base_installer_step import BaseInstallerStep
from hyperion_client.deploy_info import DeployInfo

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'tools'))
from cdh_config_tool import KuduConfigTool
from mothership_config_tool import MothershipKuduConfigTool
from config_common import MOTHERSHIP_UPDATE_CONFIG, CDH_UPDATE_CONFIG


class KuduConfigUpdateStep(BaseInstallerStep):
    """
    更新 kudu 配置
    """

    def __init__(self):
        super().__init__()

    def update(self):
        if DeployInfo().get_hadoop_distribution() == HadoopDistributionType.MOTHERSHIP:
            MothershipKuduConfigTool().do_update(MOTHERSHIP_UPDATE_CONFIG)
        elif DeployInfo().get_hadoop_distribution() == HadoopDistributionType.CLOUDERA:
            KuduConfigTool().do_update(CDH_UPDATE_CONFIG)
        else:
            # 混部场景，暂时不处理
            self.logger.info('hadoop distribution type is mix, no need to update')

    def check(self):
        # 适配 installer 通用流程
        return True
