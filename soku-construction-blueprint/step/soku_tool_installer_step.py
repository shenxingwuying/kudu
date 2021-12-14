import os
import sys

from hyperion_utils.shell_utils import check_call

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from construction_vehicle.step.base_installer_step import BaseInstallerStep

class SokuToolInstallerStep(BaseInstallerStep):
    def __init__(self):
        super().__init__()
        os.environ['SENSORS_SOKU_HOME'] = self.product_home
        self.package_dir = os.path.join(os.path.dirname(__file__), '..', '..')

    def update(self):
        # 更新系统目录下kudu工具的链接位置到soku_tool目录下，以便kudu工具进行小版本升级
        dir_kudu = os.path.join("/usr/bin", "kudu")
        if os.path.islink(dir_kudu):
            cmd = "sudo rm -f '%s'" % dir_kudu
            check_call(cmd, self.logger.debug)
            src_kudu = os.path.join(self.product_home, "soku_tool/kudu")
            cmd = "sudo ln -s '%s' '%s'" % (src_kudu, dir_kudu)
            check_call(cmd, self.logger.debug)
        dir_sp_kudu = os.path.join("/usr/bin", "sp_kudu")
        if os.path.islink(dir_sp_kudu):
            cmd = "sudo rm -f '%s'" % dir_sp_kudu
            check_call(cmd, self.logger.debug)
            src_sp_kudu = os.path.join(self.product_home, "soku_tool/sp_kudu")
            cmd = "sudo ln -s '%s' '%s'" % (src_sp_kudu, dir_sp_kudu)
            check_call(cmd, self.logger.debug)        

    def check(self):
        # 适配 installer 通用流程，不需要校验
        return True