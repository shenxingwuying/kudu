#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import params
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()
home_dir = config['source_params']['home_path']
kudu_home = os.path.join(home_dir, config['source_params']['binary_dir'])
# 在标准集群环境下，meta、data分离
# data节点上的tserver启动时会用本文件获取master信息
# 而data上没有meta目录，所以这里是空值，并且后面也不会用这个
meta_dir = config['configurations']['cluster_info']['meta_dir']
if len(config['configurations']['cluster_info']['meta_dir']) > 0:
    meta_dir = config['configurations']['cluster_info']['meta_dir'][0]

module_name = config['module_params']['module_name']
service_port = config['module_params']['ports']['service_port']

# master config
master_flag_config = {k: v for k, v in config['configurations']['kudu_master'].items()}

# 以下配置每次动态计算
master_flag_config['webserver_doc_root'] = params.master_binary_home + "/lib/kudu/www"
kudu_master_hosts = config['cluster_node_info']['kudu_master']['nodes']
if len(kudu_master_hosts) >= 1:
    master_port = params.master_service_port
    master_flag_config['master_addresses'] = ','.join(['%s:%s' % (h, master_port) for h in kudu_master_hosts])
if len(kudu_master_hosts) == 1:
    master_flag_config['default_num_replicas'] = 1
else:
    master_flag_config['default_num_replicas'] = 3

# 以下配置创建目录用
master_log_dir = os.path.join(params.log_dir, "master")
master_fs_data_dirs = os.path.join(meta_dir, "master_data")
master_fs_wal_dir = os.path.join(meta_dir, "master_wal")

# 需要写入配置文件中
master_flag_config['log_dir'] = master_log_dir
master_flag_config['fs_data_dirs'] = master_fs_data_dirs
master_flag_config['fs_wal_dir'] = master_fs_wal_dir
master_flag_config['rpc_bind_addresses'] = '0.0.0.0:' + str(params.master_service_port)
master_flag_config['webserver_port'] = params.master_webserver_port

# 探活用
webserver_port = params.master_service_port
