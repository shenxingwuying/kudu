#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()
module_name = config['module_params']['module_name']
log_dir = config['runtime_params']['log_dir']
runtime_dir = config['runtime_params']['runtime_dir']
kudu_conf_dir = os.path.join(runtime_dir, 'conf')
kudu_pid_dir = os.path.join(runtime_dir, 'pids')
kudu_user = config['module_params']['user']
kudu_user_group = config['module_params']['user_groups'][0]

# ports
cluster_port_info = config['cluster_port_info']
master_service_port = cluster_port_info['kudu_master_ports']['service_port']['port']
master_webserver_port = cluster_port_info['kudu_master_ports']['webserver_port']['port']
tserver_service_port = cluster_port_info['kudu_tserver_ports']['service_port']['port']
tserver_webserver_port = cluster_port_info['kudu_tserver_ports']['webserver_port']['port']
collector_webserver_port = cluster_port_info['kudu_collector_ports']['webserver_port']['port']

home_dir = config['source_params']['home_path']
shim_dir = os.path.join(home_dir, config['source_params']['shim_dir'])
kudu_home = os.path.join(home_dir, module_name)
master_binary_home = os.path.join(kudu_home, 'kudu_master')
master_bin = os.path.join(master_binary_home, 'sbin')
tserver_binary_home = os.path.join(kudu_home, 'kudu_tserver')
tserver_bin = os.path.join(tserver_binary_home, 'sbin')
collector_binary_home = os.path.join(kudu_home, 'kudu_collector')
collector_bin = os.path.join(collector_binary_home, 'sbin')
tool_home = os.path.join(collector_binary_home, 'bin')
