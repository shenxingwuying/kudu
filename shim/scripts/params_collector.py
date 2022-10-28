#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import params
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()
home_dir = config['source_params']['home_path']
kudu_home = os.path.join(home_dir, config['source_params']['binary_dir'])
module_name = config['module_params']['module_name']

# collector config
collector_flag_config = {k: v for k, v in config['configurations']['kudu_collector'].items()}

collector_flag_config['collector_cluster_name'] = "cluster"

# 以下配置每次动态计算
collector_flag_config['webserver_doc_root'] = params.collector_binary_home + "/lib/kudu/www"
kudu_master_hosts = config['cluster_node_info']['kudu_master']['nodes']
master_port = params.master_service_port
collector_flag_config['collector_master_addrs'] = ','.join(['%s:%s' % (h, master_port) for h in kudu_master_hosts])
if len(kudu_master_hosts) == 1:
    collector_flag_config['collector_monitor_table_replication_factor'] = 1
else:
    collector_flag_config['collector_monitor_table_replication_factor'] = 3

# 创建目录用
collector_log_dir = os.path.join(params.log_dir, "collector")

# 需要写入配置文件中
collector_flag_config['log_dir'] = collector_log_dir
collector_flag_config['collector_prometheus_exposer_port'] = params.collector_webserver_port

# Kerberos 相关配置
if params.enable_kerberos:
    collector_flag_config['keytab_file'] = params.keytab_file
    collector_flag_config['principal'] = params.principal_info['kudu_tool'][0]
    collector_flag_config['unlock_experimental_flags'] = 'true'
    collector_flag_config['rpc_authentication'] = 'required'
    collector_flag_config['rpc_encryption'] = 'required'

# 探活用
webserver_port = params.collector_webserver_port
