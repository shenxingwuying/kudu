import os
import sys

from hyperion_utils.shell_utils import check_call
from subprocess import getoutput
sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from construction_vehicle.step.base_installer_step import BaseInstallerStep


class SokuToolInstallerStep(BaseInstallerStep):
    def __init__(self):
        super().__init__()
        os.environ['SENSORS_SOKU_HOME'] = self.product_home
        self.package_dir = os.path.join(os.path.dirname(__file__), '..', '..')

    def update(self):
        # 更新系统目录下kudu工具的链接位置到soku_tool目录下
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
        # CDH环境下kudu会链接到该目录下，且直接替换的方式并不能使/usr/bin/kudu的优先级更高
        # 保险起见，直接将这里的kudu链接到soku_tool目录下,升级时直接替换源soku_tool目录即可
        cdh_home = getoutput("""cat ~/.bashrc | grep PATH | grep CDH | sed "s/[=:']/ /g" | awk '{print $3}'""")
        kudu_tool_path = os.path.join(cdh_home, "../../CDH/bin/kudu")
        if os.path.islink(kudu_tool_path) or os.path.isfile(kudu_tool_path):
            cmd = "sudo rm -f '%s'" % kudu_tool_path
            check_call(cmd, self.logger.debug)
            cmd = "sudo ln -s '%s' '%s'" % (src_kudu, kudu_tool_path)
            check_call(cmd, self.logger.debug)
        kudu_sensors_tool_path = os.path.join(cdh_home, "../../KUDU_SENSORS_DATA/bin/kudu")
        if os.path.islink(kudu_sensors_tool_path) or os.path.isfile(kudu_sensors_tool_path):
            cmd = "sudo rm -f '%s'" % kudu_sensors_tool_path
            check_call(cmd, self.logger.debug)
            cmd = "sudo ln -s '%s' '%s'" % (src_kudu, kudu_sensors_tool_path)
            check_call(cmd, self.logger.debug)

    def check(self):
        # 适配 installer 通用流程，不需要校验
        return True
