#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
@author dengke(dengke@sensorsdata.cn)
@brief

"""


def module_config_change(params):
    """ 修改 module config
    """
    return {}


def role_config_group_change(module_name, role_name, params):
    """ role_config_group 配置修改，需要区分 role 的 role_config_group 计算逻辑
    返回值需要在 module_configurations.yml 中声明
    """
    if role_name == 'kudu_master':
        d = kudu_master_config_group_change(params['hardware_configs'])
    elif role_name == 'kudu_tserver':
        d = kudu_tserver_config_group_change(params['hardware_configs'])
    else:
        d = {}
    return d


def kudu_master_config_group_change(hardware_params):
    return {
        'cluster_info': {
            'meta_dir': hardware_params['meta_dir']
        }
    }


def kudu_tserver_config_group_change(hardware_params):
    return {
        'cluster_info': {
            'random_dirs': hardware_params['random_dir']
        }
    }
