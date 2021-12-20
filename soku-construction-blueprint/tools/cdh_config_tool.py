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

ROLE_GROUPS = {
    'KUDU_MASTER': 'gflagfile_role_safety_valve',
    'KUDU_TSERVER': 'gflagfile_role_safety_valve'
}


class KuduConfigTool:
    def __init__(self, logger=None):
        if not logger:
            self.logger = logging.getLogger()
        else:
            self.logger = logger
        self.my_host = socket.getfqdn()
        self.hosts = DeployTopo().get_all_host_list()
        self.api = utils.sa_cm_api.SaCmAPI(logger=self.logger)

    def get_cloudera_config_setter(self):
        cloudera_client_conf = ConfigManager().get_client_conf('sp', 'cloudera')
        cm_url = cloudera_client_conf['cloudera_manager_api_url']
        root_url = '%s/api/v10' % cm_url
        return CdhConfigCommon(root_url,
                                    cloudera_client_conf['cloudera_manager_username'],
                                    cloudera_client_conf['cloudera_manager_password'],
                                    self.logger)

    def update_config(self, key, value, common_configs_json, cmd_configs_json):
        if key in cmd_configs_json:
            if value == cmd_configs_json[key]:
                return False, cmd_configs_json
            else:
                cmd_configs_json[key] = value
                return True, cmd_configs_json
        if key in common_configs_json and value == common_configs_json[key]:
            return False, cmd_configs_json
        cmd_configs_json[key] = value
        return True, cmd_configs_json

    def check_and_change_cmd_args(self, role_type, config, common_configs_json, resetter):
        need_restart_service = False
        if 'KUDU_COMMON' == role_type:
            path = '/clusters/cluster/services/kudu/config/'
            for (key, value) in config.items():
                if key not in common_configs_json:
                    need_restart_service = True
                    common_configs_json[key] = value
                elif key in common_configs_json and value != common_configs_json[key]:
                    need_restart_service = True
                    common_configs_json[key] = value
            if need_restart_service:
                new_common_configs = ''
                for (key, value) in common_configs_json.items():
                    new_common_configs += '--' + key + '=' + value + '\n'
                new_common_configs = new_common_configs.rstrip('\n')
                self.logger.info('%s: new common gflagfile = %s' % (role_type, new_common_configs))
                resetter.put_if_needed(path, 'gflagfile_service_safety_valve', new_common_configs, ('kudu:common gflagfile'))
        else:
            path = '/clusters/cluster/services/kudu/roleConfigGroups'
            for role_group in resetter.get_role_groups('kudu', role_type, path):
                self.logger.info('role_group = %s' % role_group)
                cmd_configs = resetter.get_value('%s/%s/config' % (path, role_group), ROLE_GROUPS[role_type]).split('\n')
                cmd_configs_json = {}
                for conf in cmd_configs:
                    item = conf.lstrip('-').split('=')
                    cmd_configs_json[item[0]] = item[1]
                self.logger.info('old gflagfile_role_safety_valve = [%s]' % cmd_configs_json)

                # 判断配置本身是否存在冲突，有冲突直接报错,有相同的删除group中的
                cmd_configs_json, need_restart_service = self.conf_pre_check(role_type, common_configs_json, cmd_configs_json)
                for (key, value) in config.items():
                    ret, cmd_configs_json = self.update_config(key, value, common_configs_json, cmd_configs_json)
                    need_restart_service = ret or need_restart_service
                if need_restart_service:
                    new_cmd_configs = ''
                    for (key, value) in cmd_configs_json.items():
                        new_cmd_configs += '--' + key + '=' + value + '\n'
                    new_cmd_configs = new_cmd_configs.rstrip('\n')
                    self.logger.info('%s: new gflagfile_role_safety_valve = [%s]' % (role_type, new_cmd_configs))
                    resetter.put_if_needed('%s/%s/config' % (path, role_group), ROLE_GROUPS[role_type], new_cmd_configs, ('kudu:%s' % role_group))
        return need_restart_service, common_configs_json

    def wait_service_done(self, service, timeout=600):
        '''确认服务正常 注意这里的服务正常是语义检查'''
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

    def get_config(self, resetter, path, name):
        config = resetter.get_value(path, name).split('\n')
        config_json = {}
        for conf in config:
            item = conf.lstrip('-').split('=')
            config_json[item[0]] = item[1]
        return config_json

    def conf_pre_check(self, role_type, common_configs_json, cmd_configs_json):
        need_restart = True
        for (key, value) in common_configs_json.items():
            if key in cmd_configs_json:
                if value != cmd_configs_json[key]:
                    raise Exception('[%s] update config error! [%s=%s] conflict with common gflagfile[%s=%s]' % (
                        role_type, key, cmd_configs_json[key], key, common_configs_json[key]))
                else:
                    cmd_configs_json.pop(key, None)
                    need_restart = True
        return cmd_configs_json, need_restart

    def do_update(self, update_config):
        if 1 == len(self.hosts):
            self.api.waiting_service_ready(True)
        if self.my_host == self.api.cm_host:
            is_config_change = False
            resetter = self.get_cloudera_config_setter()
            common_configs_json = self.get_config(resetter, '/clusters/cluster/services/kudu/config/', 'gflagfile_service_safety_valve')
            for (role_type, config) in update_config.items():
                ret, common_configs_json = self.check_and_change_cmd_args(role_type, config, common_configs_json, resetter)
                is_config_change = is_config_change or ret
            # 打印修改配置的日志信息
            if is_config_change:
                self.logger.warn('kudu config has been updated!')
        else:
            self.logger.info('host(%s) not cm_host(%s), skip' % (self.my_host, self.api.cm_host))
        if 1 == len(self.hosts):
            cmd = 'sudo service cloudera-scm-server stop'
            shell_utils.check_call(cmd, self.logger.debug)
