#!./python/bin/python3
# -*- coding: UTF-8 -*-
import logging
import os
import sys
import socket

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_client.config_manager import ConfigManager
from hyperion_client.deploy_info import DeployInfo
from hyperion_utils import shell_utils

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], 'admintools'))
from mothership_tools.config.mothership_config_tool import MothershipConfigTool
from mothership_tools.mothership_api import MothershipAPI

import config_common


class MothershipKuduConfigTool:
    def __init__(self, logger=None):
        if not logger:
            self.logger = logging.getLogger()
        else:
            self.logger = logger
        self.local_host = socket.getfqdn()
        self.mothership_api = MothershipAPI(logger=self.logger)
        # 获取 mothership 的 client conf 找到 server 的机器
        self.server_api_url = ConfigManager().get_client_conf_by_key('sp', 'mothership', 'mothership_server_api_url')
        self.is_simplified_cluster = DeployInfo().get_simplified_cluster()
        self.random_dirs_count = config_common.get_host_random_dirs_count(self.local_host)
        self.host_mem_gb = config_common.get_host_mem_gb(self.local_host)

    def _check_and_canonicalize_module_name(self, module_name):
        if not module_name:
            raise Exception('please specify module!')
        all_module_list = self.mothership_api.get_service_names()
        module_name = module_name.upper()
        if module_name not in all_module_list:
            raise Exception('invalid module[%s], should be one of %s' % (module_name, all_module_list))
        return module_name

    def _check_and_change_cmd_args(self, role_type, key, value):
        if 'KUDU_MASTER' == role_type:
            role = 'kudu-master'
        elif 'KUDU_TSERVER' == role_type:
            role = 'kudu-tserver'
        else:
            raise Exception('invalid role [%s]!, check!' % (role_type))

        need_update = False
        key_exist = False
        config_group_name = 'Default'
        config_tool = MothershipConfigTool(self.mothership_api)
        module = self._check_and_canonicalize_module_name('kudu')
        for config_group_name, old_value in config_tool.get_config_from_all_config_group(module, role, key):
            # key 已存在
            key_exist = True
            # value 发生了变化
            if old_value.lower() != value:
                need_update = True
                break
        # 需要更新配置
        if need_update or not key_exist:
            self.logger.info('%s: config (%s) not exist or not conform, reset to %s' % (role_type, key, value))
            shell_utils.check_call(
                'spadmin mothership config set -m kudu -c {} -g {} -n {} -v {}'
                ' --comment \"update config\" '.format(role, config_group_name, key, value))
        else:
            self.logger.info('%s: config (%s) is conform, no need to reset' % (role_type, key))

    # spadmin mothership config set -m kudu -c kudu-tserver -g Default -n rpc_authentication -v disabled --comment 'change_information'
    def do_update(self, new_roles_config_dict):
        if self.local_host not in self.server_api_url:
            self.logger.info('%s is not mothership server host(%s), skip update' % (
                self.local_host, self.server_api_url))
            return
        for (role, new_config_dict) in new_roles_config_dict.items():
            for (key, value) in new_config_dict.items():
                if len(value) == 0:
                    value = config_common.get_dynamic_config_value(key, self.is_simplified_cluster,
                                                                   self.random_dirs_count, self.host_mem_gb)
                self._check_and_change_cmd_args(role, key, value)
