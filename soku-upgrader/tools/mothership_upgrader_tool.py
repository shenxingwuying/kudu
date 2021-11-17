import os
import sys

sys.path.append(os.path.dirname(__file__))
from base_upgrader_tool import BaseUpgraderTool
from hyperion_utils import shell_utils
from hyperion_utils import yaml_utils
from hyperion_client.config_manager import ConfigManager

class MothershipUpgraderTool(BaseUpgraderTool):
    def __init__(self, logger, package_dir):
        super().__init__(logger, package_dir)
        self.mothership_tar = ''
        for file in os.listdir(self.package_dir):
            if file.startswith('kudu-') and file.endswith('.tar'):
                self.mothership_tar = file
                break
        # 获取mothership的client conf 找到server的机器
        self.server_api_url = ConfigManager().get_client_conf_by_key('sp', 'mothership', 'mothership_server_api_url')

    def get_mothership_update_tar_if_not_exist(self):
        if len(self.mothership_tar) == 0:
            raise Exception('connot find mothership update tar')
        else:
            return self.mothership_tar, os.path.join(self.package_dir, self.mothership_tar)

    def update(self):
        if self.my_host not in self.server_api_url:
            self.logger.info('{} is not mothership server host, skip update'.format(self.my_host))
            return
        update_tar_name, update_tar_full_path = self.get_mothership_update_tar_if_not_exist()
        # 获取本地安装版本
        sdp_version_path = os.path.join(os.environ['SENSORS_PLATFORM_HOME'], 'sdp', 'sdp-version.yml')
        sdp_version_yml = yaml_utils.read_yaml(sdp_version_path)
        remote_fields = sdp_version_yml['module']['kudu']['version'].split('-')
        remote_full_version = remote_fields[1]
        local_fields = update_tar_name.split('-')
        local_full_version = local_fields[1]
        if not self.compare_version(local_full_version, remote_full_version):
            self.logger.info('skip update kudu, local_version(%s) < remote_version(%s)' % (local_full_version, remote_full_version))
            return
        self.logger.info('start update kudu, version(%s->%s)' % (remote_full_version, local_full_version))
        shell_utils.check_call('spadmin mothership upgrader -m kudu -p {}'.format(update_tar_full_path))

    def rollback(self):
        if self.my_host not in self.server_api_url:
            self.logger.info('{} is not mothership server host, skip rollback'.format(self.my_host))
            return
        shell_utils.check_call('spadmin mothership upgrader -m kudu -r')
