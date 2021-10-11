import os
import sys
import socket

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))

class BaseUpgraderTool:
    def __init__(self, logger, package_dir):
        self.logger = logger
        self.package_dir = package_dir
        self.my_host = socket.getfqdn()

    # soku 版本一定是 dragon 版本，远端版本可能是 dragon 版本，也可能是 sensors-package 版本
    def compare_version(self, local_full_version, remote_full_version):
        self.logger.info('local_full_version = %s, remote_full_version = %s' % (local_full_version, remote_full_version))
        local_version = local_full_version.split('-')[0]
        remote_version = remote_full_version.split('-')[0]
        # 判断下是否是旧版->dragon或者dragon->dragon
        if len(local_version.split('.')) == 4 and len(remote_version.split('.')) == 3:
            # 旧版升dragon
            return True
        if len(local_version.split('.')) == 4 and len(remote_version.split('.')) == 4:
            # dragon升dragon
            return self.check_dragon_version(local_version, remote_version)
        return False

    def check_dragon_version(self, local_version, remote_version):
        # 两个都是四位版本号
        local_version_list = local_version.split('.')
        remote_version_list = remote_version.split('.')
        for i in range(4):
            if int(local_version_list[i]) > int(remote_version_list[i]):
                return True
            elif int(local_version_list[i]) < int(remote_version_list[i]):
                return False
        return False
