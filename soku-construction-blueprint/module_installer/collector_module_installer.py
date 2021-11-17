#!/bin/env python
# -*- coding: UTF-8 -*-

import copy
import os
import sys
import time

from utils import shell_wrapper
import utils.sa_utils
import utils.config_manager

from construction_vehicle.module_installer.external_module_installer import ExternalModuleInstaller
from hyperion_utils.shell_utils import check_call
from hyperion_utils import shell_utils

class CollectorModuleInstaller(ExternalModuleInstaller):
    def _calc_final_gflag_kv(self, role_name):
        """计算某个机器上某个角色最终的gflag的kv值"""
        server_conf = self.module.get_conf_by_type('server_conf')
        common_gflags = server_conf['common']
        gflags = copy.copy(common_gflags)
        role_short_name = role_name.split('_')[1]  # kudu_collector -> collector
        gflags.update(server_conf[role_short_name])
        gflags.update(server_conf[role_short_name + '_host'].get(self.my_host, {}))
        kudu_client_conf = utils.config_manager.get_kudu_client_conf()
        master_addrs = kudu_client_conf['master_address']
        # 更新master地址
        if role_name == 'kudu_collector' :
            gflags['collector_master_addrs'] = master_addrs
        return gflags

    def _get_service_port(self):
        gflags = self._calc_final_gflag_kv('kudu_collector')
        return gflags['collector_prometheus_exposer_port']

    def make_links(self):
        """建立对应的软链"""
        links_to_make = []
        soku_home = self.get_product_home_dir()
        self.logger.info('dirs to make links: %s' % soku_home)
        # kudu-collector
        src = os.path.join(soku_home, "collector/sbin", 'kudu-collector')
        dst = os.path.join("/usr/bin", 'kudu-collector')
        links_to_make.append((src, dst, True))
        # tool
        tool_src = os.path.join(soku_home, "collector/bin", 'kudu')
        tool_dst = os.path.join(soku_home, "collector/sbin", 'kudu')
        links_to_make.append((tool_src, tool_dst, False))
        # 创建软链
        for src, dst, issudo in links_to_make:
            if os.path.islink(dst):
                cmd = "rm -f '%s'" % dst
                if issudo:
                    cmd = 'sudo ' + cmd
                check_call(cmd, self.logger.debug)
            cmd = "ln -s '%s' '%s'" % (src, dst)
            if issudo:
                cmd = 'sudo ' + cmd
            check_call(cmd, self.logger.debug)

    def dirs_to_make(self):
        ''' collector conf的程序目录是现造的'''
        ret = [os.path.join(self.get_product_home_dir(), 'conf')]
        final_gflag_kv = self._calc_final_gflag_kv('kudu_collector')
        ret.append(final_gflag_kv['log_dir'])
        self.logger.info('dirs to make: %s' % ret)
        return ret

    def make_dirs(self):
        """根据dirs_to_make()返回创建目录"""
        for d in self.dirs_to_make():
            if os.path.exists(d) :
                continue
            os.makedirs(d)
            self.logger.info('make dir %s' % d)

    def check_alive_by_port(self, port):
        """判断端口是否被占用，返回true表示占用"""
        command = "sudo lsof -t -i tcp:{port} -s tcp:LISTEN".format(port=port)
        result = utils.shell_wrapper.run_cmd(command, self.logger.info)
        if result['ret'] == 0:
            return True
        else:
            return False

    def do_checkalive(self):
        """回调函数 检查服务是否起来"""
        self.check_alive_by_port(self._get_service_port())

    def do_start(self):
        collector_home = os.path.join(self.get_product_home_dir(), 'collector')
        cmd = 'cd %s && sh start_kudu.sh collector' % collector_home
        shell_utils.check_call(cmd, self.logger.debug)

    def do_stop(self):
        server_port = self._get_service_port()
        if self.check_alive_by_port(server_port):
            self.logger.info('Kudu-collector on port %d is running. Try to stop it' % server_port)
            # 根据端口号来kill
            cmd = 'lsof -i tcp:%d -s tcp:LISTEN' % server_port
            l = utils.shell_wrapper.check_output(cmd).splitlines()
            fields = l[1].split()
            pid, user = int(fields[1]), fields[2]
            if user.lower() not in ['sa_standalone', 'sa_cluster']:
                raise Exception('port %d is occupied by unknown process[%d] user[%s]' \
                                % (server_port, pid, user))
            cmd = 'kill -9 %d' % pid
            utils.shell_wrapper.check_call(cmd, self.logger.debug)
        else:
            self.logger.info('Kudu-collector not running.')

    def start_service(self):
        """根据do_checkalive()和do_start() 启动服务并检查"""
        self.do_start()
        if self.check_alive_by_port(self._get_service_port()):
            self.logger.debug('Kudu-collector success to start.')
        else:
            self.logger.error('Kudu-collector fail to start.')

    def add_soku(self):
        os.environ['SENSORS_SOKU_HOME'] = self.product_home
        cmd = 'source ~/.bashrc'
        check_call(cmd, self.logger.debug)
        self.logger.info('soku_tool installer SENSORS_SOKU_HOME %s' % os.environ['SENSORS_PLATFORM_HOME'])

    def install_module_progress(self):
        """
        得到实际的各个步骤
        :return: 返回值是一个二元组
        """
        return [
            ('Store module', self.store_module),
            ('Add soku to os environ', self.add_soku),
            ('Stop service if needed', self.do_stop),
            ('Make kudu-collector dirs', self.make_dirs),
            ('Make links', self.make_links),
            ('Generate conf', self.deploy_configure_file),
            ('Start service', self.start_service)
        ]

    def deploy_configure_file(self):
        """写gflag文件"""
        collector_conf_home = os.path.join(self.get_product_home_dir(), 'conf')
        with open(os.path.join(collector_conf_home, '%s.gflagfile' % 'kudu_collector'), 'w+') as f:
            for k, v in self._calc_final_gflag_kv('kudu_collector').items():
                if v is None:
                    f.write('--%s\n' % k)
                else:
                    f.write('--%s=%s\n' % (k, v))
