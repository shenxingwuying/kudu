import os
import sys

sys.path.append(os.environ['SENSORS_PLATFORM_HOME'])
from construction_blueprint.installer_constants import HadoopDistributionType

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from construction_vehicle.step.base_installer_step import BaseInstallerStep
from hyperion_client.deploy_info import DeployInfo

sys.path.append(os.path.join(os.path.dirname(__file__), '../..', 'upgrader', 'tools'))
from mothership_upgrader_tool import MothershipUpgraderTool
from cdh_upgrader_tool import CdhUpgraderTool


class SokuInstallerStep(BaseInstallerStep):
    def __init__(self):
        super().__init__()
        os.environ['SENSORS_SOKU_HOME'] = self.product_home
        self.package_dir = os.path.join(os.path.dirname(__file__), '..', '..')

    def update(self):
        if DeployInfo().get_hadoop_distribution() == HadoopDistributionType.MOTHERSHIP:
            mothershipUpgraderTool = MothershipUpgraderTool(self.logger, self.package_dir)
            mothershipUpgraderTool.update()
        elif DeployInfo().get_hadoop_distribution() == HadoopDistributionType.CLOUDERA:
            cdhUpgraderTool = CdhUpgraderTool(self.logger, self.package_dir)
            cdhUpgraderTool.update()
        else:
            # 混部场景，暂时不处理
            self.logger.info('hadoop distribution type is mix, no need to update')

    def check(self):
        # 适配 installer 通用流程，不需要校验
        return True
