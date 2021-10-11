#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import utils.shell_wrapper

from construction_vehicle.module_assembler.external_module_assembler import ExternalModuleAssembler

class CollectorModuleAssembler(ExternalModuleAssembler):
    def _get_cluster_id(self):
        """简单获取customer_id"""
        customer_id = utils.shell_wrapper.check_output('spadmin config get global -n customer_id -c', self.logger.debug)
        return customer_id

    def _is_standalone(self):
        """简单判断是否单机,从ZK读取"""
        return self.runtime_conf.cluster.global_conf.simplified_cluster

    def custom_module_get_common_gflags(self):
        """所有gflags文件的通用配置"""
        return {
            'disable_core_dumps': True,
            'log_force_fsync_all': False,
            'logbuflevel': 0,
            'max_cell_size_bytes': 262144,
            'max_clock_sync_error_usec': 100000000,
            'max_log_size': 10,
            'minloglevel': 0,
            'superuser_acl': None,
            'unlock_unsafe_flags': True,
            'user_acl': '*',
            'v': 0,
            'webserver_certificate_file': None,
            'webserver_private_key_file': None,
            'webserver_private_key_password_cmd': None,
            'webserver_doc_root': os.path.join(self.get_product_home_dir(), 'collector/lib/kudu/www'),
            'rpc_authentication': 'disabled',
            'rpc_encryption': 'disabled',
        }

    def custom_module_get_collector_gflags(self):
        """collector gflag的通用配置"""
        return {
            'collector_report_method' : 'prometheus',
            'collector_simplify_hostname' : True,
            'collector_monitor_table_replication_factor' : 1 if self._is_standalone() else 3,
            'log_dir' : os.path.join(self.get_product_home_dir(), 'logs/collector'),
            'collector_prometheus_exposer_port' : 9050,
            'collector_monitor_avg_tablets_count_per_node' : 3,
            'collector_monitor_avg_record_count_per_tablet' : 100,
            'collector_cluster_name' : self._get_cluster_id(),
        }

    def custom_module_get_collector_host_gflags(self):
        """获取单个主机自定义的collector配置 此处占位返回空"""
        return {}
