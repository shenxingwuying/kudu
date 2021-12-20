#!./python/bin/python3
# -*- coding: UTF-8 -*-
import logging
import os
import sys
import socket

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_client.config_manager import ConfigManager
from hyperion_utils import shell_utils

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], 'admintools'))
from mothership_tools.config.mothership_config_tool import MothershipConfigTool
from mothership_tools.mothership_api import MothershipAPI


class MothershipKuduConfigTool:
    def __init__(self, logger=None):
        if not logger:
            self.logger = logging.getLogger()
        else:
            self.logger = logger
        self.my_host = socket.getfqdn()
        self.mothership_api = MothershipAPI(logger=self.logger)
        # 获取 mothership 的 client conf 找到 server 的机器
        self.server_api_url = ConfigManager().get_client_conf_by_key('sp', 'mothership', 'mothership_server_api_url')

    def _check_and_clean_module(self, module_name):
        if not module_name:
            raise Exception('please specify module!')
        all_module_list = self.mothership_api.get_service_names()
        module_name = module_name.upper()
        if module_name not in all_module_list:
            raise Exception('invalid module[%s], should be one of %s' % (module_name, all_module_list))
        return module_name

    def _check_and_change_cmd_args(self, role_type, key, value):
        config_reset = False
        key_exist = False
        config_group_name = 'Default'
        config_tool = MothershipConfigTool(self.mothership_api)
        module = self._check_and_clean_module('kudu')
        if 'KUDU_MASTER' == role_type:
            role = 'kudu-master'
        elif 'KUDU_TSERVER' == role_type:
            role = 'kudu-tserver'
        else:
            raise Exception('invalid role_type [%s]!, check!' % (role_type))
        for config_group_name, config_value in config_tool.get_config_from_all_config_group(module, role, key):
            key_exist = True
            if config_value.lower() != value:
                config_reset = True
                break
        if config_reset is True or key_exist is not True:
            self.logger.info('%s: config (%s) not exist or not conform, reset to %s' % (role_type, key, value))
            shell_utils.check_call(
                'spadmin mothership config set -m kudu -c {} -g {} -n {} -v {}'
                ' --comment \"update config\" '.format(role, config_group_name, key, value))
            return True
        else:
            self.logger.info('%s: config (%s) is conform, no need to reset' % (role_type, key))
            return False

    # spadmin mothership config set -m kudu -c kudu-tserver -g Default -n rpc_authentication -v disabled --comment 'change_information'
    def do_update(self, update_config):
        if self.my_host not in self.server_api_url:
            self.logger.info('%s is not mothership server host(%s), skip update' % (
                self.my_host, self.server_api_url))
            return
        for (role_type, config) in update_config.items():
            is_config_change = False
            for (k, v) in config.items():
                reset = self._check_and_change_cmd_args(role_type, k, v)
                if reset:
                    is_config_change = True
            if is_config_change:
                self.logger.warn('%s config has been updated.' % (role_type))
