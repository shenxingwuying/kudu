#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
@author dengke(dengke@sensorsdata.cn)
@brief

"""


def change(params):
    kudu_master_hosts = params['cluster_node_info']['kudu_master_nodes']
    master_port = params['cluster_port_info']['kudu_master_ports']['service_port']['port']
    tserver_master_addrs = []
    for master_host in kudu_master_hosts:
        tserver_master_addrs.append(master_host + ':' + str(master_port))
    return {
        'master_address': tserver_master_addrs
    }
