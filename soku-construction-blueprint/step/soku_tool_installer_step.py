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
        # 防止系统目录下存在kudu工具
        dir_kudu = os.path.join("/usr/bin", "kudu")
        if os.path.islink(dir_kudu):
            cmd = "sudo rm -f '%s'" % dir_kudu
            check_call(cmd, self.logger.debug)
        dir_sp_kudu = os.path.join("/usr/bin", "sp_kudu")
        if os.path.islink(dir_sp_kudu):
            cmd = "sudo rm -f '%s'" % dir_sp_kudu
            check_call(cmd, self.logger.debug)
        # 添加kudu工具的路径到系统环境变量中
        os_path = os.path.join(self.product_home, 'soku_tool')
        cmd = "export PATH=$PATH:{kudu_src}".format(kudu_src=os.path.join(self.product_home, 'soku_tool'))
        check_call(cmd, self.logger.debug)
        cmd = '''echo "export PATH=%s:\$PATH" >> /home/sa_cluster/.bashrc''' % os_path
        check_call(cmd, self.logger.debug)
        kudu_home_src = "/home/kudu"
        kudu_lib_src = "/var/lib/kudu"
        if os.path.exists(kudu_home_src):
            cmd = "sudo cp -f /home/sa_cluster/.bashrc %s/.bashrc" % kudu_home_src
            check_call(cmd, self.logger.debug)
        elif os.path.exists(kudu_lib_src):
            cmd = "sudo cp -f /home/sa_cluster/.bashrc %s/.bashrc" % kudu_lib_src
            check_call(cmd, self.logger.debug)

    def check(self):
        # 适配 installer 通用流程，不需要校验
        return True