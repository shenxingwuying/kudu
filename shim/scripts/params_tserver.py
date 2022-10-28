#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import params
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()
home_dir = config['source_params']['home_path']
kudu_home = os.path.join(home_dir, config['source_params']['binary_dir'])
random_dirs = config['configurations']['cluster_info']['random_dirs']
module_name = config['module_params']['module_name']

# tserver config
tserver_flag_config = {k: v for k, v in config['configurations']['kudu_tserver'].items()}

# 以下配置每次动态计算
tserver_flag_config['webserver_doc_root'] = params.tserver_binary_home + "/lib/kudu/www"
kudu_master_hosts = config['cluster_node_info']['kudu_master']['nodes']
master_port = params.master_service_port
tserver_flag_config['tserver_master_addrs'] = ','.join(['%s:%s' % (h, master_port) for h in kudu_master_hosts])

# 单机情况下设置成0，集群为1
# 判断单机目前用集群中kudu-master的数量
if len(kudu_master_hosts) > 1:
    tserver_flag_config['maintenance_manager_num_threads'] = 1

# 以下几个配置创建目录用
tserver_log_dir = os.path.join(params.log_dir, "tserver")
tserver_fs_data_dirs = ','.join([os.path.join(random_dir, 'tserver_data') for random_dir in random_dirs])
tserver_fs_wal_dir = os.path.join(random_dirs[0], 'tserver_wal')

# 需要写入配置文件中
tserver_flag_config['log_dir'] = tserver_log_dir
tserver_flag_config['fs_data_dirs'] = tserver_fs_data_dirs
tserver_flag_config['fs_wal_dir'] = tserver_fs_wal_dir
tserver_flag_config['rpc_bind_addresses'] = '0.0.0.0:' + str(params.tserver_service_port)
tserver_flag_config['webserver_port'] = params.tserver_webserver_port

# Kerberos 相关配置
if params.enable_kerberos:
    tserver_flag_config['keytab_file'] = params.keytab_file
    tserver_flag_config['rpc_authentication'] = 'required'
    tserver_flag_config['rpc_encryption'] = 'required'
# ranger 相关配置
if params.enable_ranger:
    tserver_flag_config['tserver_enforce_access_control'] = True

# 探活用
webserver_port = params.tserver_webserver_port
