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

    def get_role_config_groups(self, resetter, service, role_type, name):
        return resetter.get_role_config(service, role_type, name)

    def change_cmd_args(self, command, key, value):
        item = command.lstrip('-').split('=')
        if key == item[0]:
            if item[1].lower() != value.lower():
                command = '-' + key + '=' + value
        return command

    def find_relative_service(self):
        # 获取相关的服务
        services = {}
        r = self.api.get_parcel_usage()
        for h in r['racks'][0]['hosts']:
            for role in h['roles']:
                if role['roleRef']['serviceName'] in services:
                    continue
                for parcel_ref in role['parcelRefs']:
                    if parcel_ref['parcelName'].lower().startswith('kudu'):
                        services[role['roleRef']['serviceName']] = parcel_ref['parcelName']
        return services

    def check_and_change_cmd_args(self, role_type, key, value):
        resetter = self.get_cloudera_config_setter()
        path = '/clusters/cluster/services/kudu/roleConfigGroups'
        need_restart_service = False
        for role_group in resetter.get_role_groups('kudu', role_type, path):
            self.logger.info('role_group = %s' % role_group)
            cmd_configs = resetter.get_value('%s/%s/config' % (path, role_group), ROLE_GROUPS[role_type]).split('\n')
            self.logger.info('old gflagfile_role_safety_valve = [%s]' % cmd_configs)
            new_cmd_configs = ''
            new_key = True
            for cmd_config in cmd_configs:
                if key in cmd_config:
                    new_cmd_config = self.change_cmd_args(cmd_config, key, value)
                    if new_cmd_config != cmd_config:
                        need_restart_service = True
                    new_key = False
                else:
                    new_cmd_config = cmd_config
                new_cmd_configs += new_cmd_config + '\n'
            if new_key is True:
                new_cmd_configs += '--' + key + '=' + value + '\n'
                need_restart_service = True
            new_cmd_configs = new_cmd_configs.rstrip('\n')
            self.logger.info('new gflagfile_role_safety_valve = [%s]' % new_cmd_configs)
            resetter.put_if_needed('%s/%s/config' % (path, role_group), ROLE_GROUPS[role_type], new_cmd_configs, ('kudu:%s' %  role_group))
        return need_restart_service

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

    def do_update(self, update_config):
        flag = False
        if 1 == len(self.hosts):
            self.api.waiting_service_ready(True)
        if self.my_host == self.api.cm_host:
            for (role_type, config) in update_config.items():
                for (k, v) in config.items():
                    ret = self.check_and_change_cmd_args(role_type, k, v)
                    flag = ret
            # 重启服务
            if flag:
                for service in self.find_relative_service():
                    self.api.post_service_command(service, 'restart', wait=True)
                    # 确认服务启动正常
                    self.wait_service_done(service, timeout=20*60)
        if 1 == len(self.hosts):
            cmd = 'sudo service cloudera-scm-server stop'
            shell_utils.check_call(cmd, self.logger.debug)
        else:
            self.logger.info('host(%s) not cm_host(%s), skip' % (self.my_host, self.api.cm_host))
