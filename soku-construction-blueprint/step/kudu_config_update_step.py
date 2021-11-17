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

# maintenance_manager_num_flush_threads，单机情况下设置成1，集群为0
# rpc_authentication=disabled
# rpc_encryption=disabled
# unlock_experimental_flags=true

UPDATE_CONFIG = {
    'KUDU_MASTER': {
        'rpc_authentication': 'disabled',
        'rpc_encryption': 'disabled'
    },
    'KUDU_TSERVER': {
        'rpc_authentication': 'disabled',
        'rpc_encryption': 'disabled',
        'unlock_experimental_flags': 'true',
        'maintenance_manager_num_flush_threads': '1' if DeployInfo().get_simplified_cluster() else '0'
    }
}


class KuduConfigUpdateStep(BaseInstallerStep):
    """
    更新 kudu 配置，并重启 kudu
    """
    def __init__(self):
        super().__init__()

    def update(self):

        if DeployInfo().get_hadoop_distribution() == HadoopDistributionType.MOTHERSHIP:
            MothershipKuduConfigTool().do_update(UPDATE_CONFIG)
        elif DeployInfo().get_hadoop_distribution() == HadoopDistributionType.CLOUDERA:
            KuduConfigTool().do_update(UPDATE_CONFIG)
        else:
            # 混部场景，暂时不处理
            self.logger.info('hadoop distribution type is mix, no need to update')

    def check(self):
        # 适配 installer 通用流程
        return True
