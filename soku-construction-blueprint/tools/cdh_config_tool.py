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
from hyperion_client.deploy_info import DeployInfo
from hyperion_utils import shell_utils

import utils.sa_cm_api

import config_common


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
        self.is_simplified_cluster = DeployInfo().get_simplified_cluster()
        self.api = utils.sa_cm_api.SaCmAPI(logger=self.logger)
        # 单机环境必须先开启cloudera-scm-server才能去获取配置，
        # 开启cloudera-scm-server前必须确保内存足够
        if self.is_simplified_cluster:
            config_common.start_cdh_server(self.api)
        self.tserver_random_dirs_count = config_common.get_role_random_dirs_count(self.api, 'kudu_tserver', 'cdh')
        self.tserver_mem_gb = config_common.get_role_mem_gb(self.api, 'kudu_tserver', 'cdh')

    def get_cloudera_config_setter(self):
        cloudera_client_conf = ConfigManager().get_client_conf('sp', 'cloudera')
        cm_url = cloudera_client_conf['cloudera_manager_api_url']
        root_url = '%s/api/v10' % cm_url
        return CdhConfigCommon(root_url,
                               cloudera_client_conf['cloudera_manager_username'],
                               cloudera_client_conf['cloudera_manager_password'],
                               self.logger)

    def update_config(self, role, key, value, old_common_config_dict, old_role_config_dict, old_role_assign_config_dict):
        if key in old_role_config_dict or key in old_common_config_dict or key in old_role_assign_config_dict:
            return False, old_role_config_dict
        old_role_config_dict[key] = value
        return True, old_role_config_dict

    def check_and_change_cmd_args(self, role, new_config_dict, old_common_config_dict, cloudera_config_setter):
        need_update = False
        setter_path = SETTER_ROLE_PATH[role]
        setter_name = SETTER_ROLE_NAME[role]
        if 'KUDU_COMMON' == role:
            old_master_config_dict = self.get_role_config_dict(cloudera_config_setter, 'KUDU_MASTER')
            old_tserver_config_dict = self.get_role_config_dict(cloudera_config_setter, 'KUDU_TSERVER')
            for (key, value) in new_config_dict.items():
                if key in old_master_config_dict or key in old_tserver_config_dict:
                    self.logger.info('%s: the conf item [ %s ] already exists in role conf, no update'
                                     % (role, key))
                if len(value) == 0:
                    value = config_common.get_dynamic_config_value(key, self.is_simplified_cluster, self.tserver_random_dirs_count, self.tserver_mem_gb)
                if key not in old_common_config_dict:
                    need_update = True
                    old_common_config_dict[key] = value
                elif value == old_common_config_dict[key]:
                    self.logger.info('%s: conf (%s: %s) already exists, and the value is the same, no update'
                                     % (role, key, value))
                else:
                    self.logger.info('%s: conf (%s) already exists, but the value is inconsistent, no update,'
                                     ' old_val: %s, new_val: %s' % (role, key, old_common_config_dict[key], value))
            if need_update:
                new_common_configs = '\n'.join('--' + key + '=' + value for key, value in old_common_config_dict.items())
                self.logger.info('%s: new common gflagfile = %s' % (role, new_common_configs))
                cloudera_config_setter.put_if_needed(setter_path, setter_name, new_common_configs, ('kudu:common gflagfile'))
        else:
            # cloudera_config_setter.get_role_groups 的返回值是什么？
            old_role_config_dict = self.get_role_config_dict(cloudera_config_setter, role)
            self.logger.info('%s: old gflagfile_role_safety_valve = [%s]' % (role, old_role_config_dict))
            for role_group in cloudera_config_setter.get_role_groups('kudu', role, setter_path):
                old_role_assign_config_dict = cloudera_config_setter.get_all_assign_item_value('%s/%s/config' % (setter_path, role_group))
                if old_role_assign_config_dict.get(setter_name) is not None:
                    del old_role_assign_config_dict[setter_name]

                # 判断配置本身是否存在冲突, 有冲突直接报错, 有相同的删除 group 中的
                old_role_config_dict, need_update = self.check_old_config(role, old_common_config_dict, old_role_config_dict)
                for (key, value) in new_config_dict.items():
                    if len(value) == 0:
                        value = config_common.get_dynamic_config_value(key, self.is_simplified_cluster, self.tserver_random_dirs_count, self.tserver_mem_gb)
                    # 检查配置项是否已经存在设置过, 设置过则不更新
                    need_update_value, old_role_config_dict = self.update_config(role, key, value, old_common_config_dict, old_role_config_dict, old_role_assign_config_dict)
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
                # 默认服务起来就 ok
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

    def get_role_config_dict(self, cloudera_config_setter, role):
        setter_path = SETTER_ROLE_PATH[role]
        setter_name = SETTER_ROLE_NAME[role]
        for role_group in cloudera_config_setter.get_role_groups('kudu', role, setter_path):
            self.logger.info('role_group = %s' % role_group)
            setter_role_path = '%s/%s/config' % (setter_path, role_group)
            return self.get_config_dict(cloudera_config_setter, setter_role_path, setter_name)

    def check_old_config(self, role, old_common_config_dict, old_role_config_dict):
        need_update = False
        for (key, value) in old_common_config_dict.items():
            if key in old_role_config_dict:
                if value != old_role_config_dict[key]:
                    self.logger.warning('[%s] old config warn! [%s=%s] conflict with common gflagfile[%s=%s]' % (
                        role, key, old_role_config_dict[key], key, old_common_config_dict[key]))
                else:
                    old_role_config_dict.pop(key, None)
                    need_update = True
        return old_role_config_dict, need_update

    def do_update(self, new_roles_config_dict):
        if self.local_host == self.api.cm_host:
            cloudera_config_setter = self.get_cloudera_config_setter()
            old_common_config_dict = self.get_config_dict(cloudera_config_setter, SETTER_ROLE_PATH['KUDU_COMMON'], SETTER_ROLE_NAME['KUDU_COMMON'])
            for (role, new_config_dict) in new_roles_config_dict.items():
                old_common_config_dict = self.check_and_change_cmd_args(role, new_config_dict, old_common_config_dict, cloudera_config_setter)
        else:
            self.logger.info('host(%s) not cm_host(%s), skip' % (self.local_host, self.api.cm_host))
        if self.is_simplified_cluster:
            cmd = 'sudo service cloudera-scm-server stop'
            shell_utils.check_call(cmd, self.logger.debug)
