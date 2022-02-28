#!/bin/env python
# -*- coding: UTF-8 -*-
"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
"""
import logging
import os
import sys
import datetime
import time
import traceback
import socket
from cdh_config_common import CdhConfigCommon

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_client.config_manager import ConfigManager
from hyperion_client.deploy_topo import DeployTopo
from hyperion_utils import shell_utils

import utils.sa_cm_api

SETTER_ROLE_NAME = {
    'KUDU_COMMON': 'gflagfile_service_safety_valve',
    'KUDU_MASTER': 'gflagfile_role_safety_valve',
    'KUDU_TSERVER': 'gflagfile_role_safety_valve'
}

SETTER_ROLE_PATH = {
    'KUDU_COMMON': '/clusters/cluster/services/kudu/config/',
    'KUDU_MASTER': '/clusters/cluster/services/kudu/roleConfigGroups',
    'KUDU_TSERVER': '/clusters/cluster/services/kudu/roleConfigGroups'
}


class KuduConfigTool:
    def __init__(self, logger=None):
        if not logger:
            self.logger = logging.getLogger()
        else:
            self.logger = logger
        self.local_host = socket.getfqdn()
        self.all_host_list = DeployTopo().get_all_host_list()
        self.api = utils.sa_cm_api.SaCmAPI(logger=self.logger)

    def get_cloudera_config_setter(self):
        cloudera_client_conf = ConfigManager().get_client_conf('sp', 'cloudera')
        cm_url = cloudera_client_conf['cloudera_manager_api_url']
        root_url = '%s/api/v10' % cm_url
        return CdhConfigCommon(root_url,
                               cloudera_client_conf['cloudera_manager_username'],
                               cloudera_client_conf['cloudera_manager_password'],
                               self.logger)

    def update_config(self, key, value, old_common_config_dict, old_role_config_dict):
        if key in old_role_config_dict:
            if value == old_role_config_dict[key]:
                return False, old_role_config_dict
            else:
                old_role_config_dict[key] = value
                return True, old_role_config_dict
        if key in old_common_config_dict and value == old_common_config_dict[key]:
            return False, old_role_config_dict
        old_role_config_dict[key] = value
        return True, old_role_config_dict

    def check_and_change_cmd_args(self, role, new_config_dict, old_common_config_dict, cloudera_config_setter):
        need_update = False
        setter_path = SETTER_ROLE_PATH[role]
        setter_name = SETTER_ROLE_NAME[role]
        if 'KUDU_COMMON' == role:
            for (key, value) in new_config_dict.items():
                if key not in old_common_config_dict or value != old_common_config_dict[key]:
                    need_update = True
                    old_common_config_dict[key] = value
            if need_update:
                new_common_configs = '\n'.join('--' + key + '=' + value for key, value in old_common_config_dict.items())
                self.logger.info('%s: new common gflagfile = %s' % (role, new_common_configs))
                cloudera_config_setter.put_if_needed(setter_path, setter_name, new_common_configs, ('kudu:common gflagfile'))
        else:
            # cloudera_config_setter.get_role_groups 的返回值是什么？
            for role_group in cloudera_config_setter.get_role_groups('kudu', role, setter_path):
                self.logger.info('role_group = %s' % role_group)
                old_role_config_list = cloudera_config_setter.get_value('%s/%s/config' % (setter_path, role_group), setter_name).split('\n')
                old_role_config_dict = {}
                for conf in set(old_role_config_list):
                    conf = conf.strip()
                    if not conf:
                        continue
                    items = conf.lstrip('-').split('=')
                    old_role_config_dict[items[0]] = items[1]
                self.logger.info('old gflagfile_role_safety_valve = [%s]' % old_role_config_dict)

                # 判断配置本身是否存在冲突，有冲突直接报错,有相同的删除group中的
                old_role_config_dict, need_update = self.check_old_config(role, old_common_config_dict, old_role_config_dict)
                for (key, value) in new_config_dict.items():
                    need_update_value, old_role_config_dict = self.update_config(key, value, old_common_config_dict, old_role_config_dict)
                    need_update = need_update_value or need_update
                if need_update:
                    new_common_configs = '\n'.join('--' + key + '=' + value for key, value in old_role_config_dict.items())
                    self.logger.info('%s: new gflagfile_role_safety_valve = [%s]' % (role, new_common_configs))
                    cloudera_config_setter.put_if_needed('%s/%s/config' % (setter_path, role_group), setter_name, new_common_configs, ('kudu:%s' % role_group))
        return old_common_config_dict

    def wait_service_done(self, service, timeout=600):
        """确认服务正常 注意这里的服务正常是语义检查"""
        start = datetime.datetime.now()
        while (datetime.datetime.now() - start).total_seconds() < timeout:
            try:
                # 默认服务起来就 ok le
                status = self.api.get_service_status(service)
                self.logger.info('kudu service %s health %s' % (status['serviceState'], status['healthSummary']))
                if status['serviceState'] == 'STARTED':
                    return True
            except Exception:
                self.logger.info(traceback.format_exc())
            time.sleep(1)
        else:
            raise Exception('service not ready after %d seconds!' % timeout)

    def get_config_dict(self, cloudera_config_setter, path, name):
        config_list = cloudera_config_setter.get_value(path, name).split('\n')
        config_dict = {}
        for config in set(config_list):
            config = config.strip()
            if not config:
                continue
            items = config.lstrip('-').split('=')
            config_dict[items[0]] = items[1]
        return config_dict

    def check_old_config(self, role, old_common_config_dict, old_role_config_dict):
        need_update = False
        for (key, value) in old_common_config_dict.items():
            if key in old_role_config_dict:
                if value != old_role_config_dict[key]:
                    raise Exception('[%s] update config error! [%s=%s] conflict with common gflagfile[%s=%s]' % (
                        role, key, old_role_config_dict[key], key, old_common_config_dict[key]))
                else:
                    old_role_config_dict.pop(key, None)
                    need_update = True
        return old_role_config_dict, need_update

    def do_update(self, new_roles_config_dict):
        if 1 == len(self.all_host_list):
            self.api.waiting_service_ready(True)
        if self.local_host == self.api.cm_host:
            cloudera_config_setter = self.get_cloudera_config_setter()
            old_common_config_dict = self.get_config_dict(cloudera_config_setter, SETTER_ROLE_PATH['KUDU_COMMON'], SETTER_ROLE_NAME['KUDU_COMMON'])
            for (role, new_config_dict) in new_roles_config_dict.items():
                old_common_config_dict = self.check_and_change_cmd_args(role, new_config_dict, old_common_config_dict, cloudera_config_setter)
        else:
            self.logger.info('host(%s) not cm_host(%s), skip' % (self.local_host, self.api.cm_host))
        if 1 == len(self.all_host_list):
            cmd = 'sudo service cloudera-scm-server stop'
            shell_utils.check_call(cmd, self.logger.debug)
