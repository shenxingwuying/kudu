#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import requests
import signal
import subprocess

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import check_process_status
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.libraries.functions.check_process_status import wait_process_stopped
from resource_management.libraries.resources.xml_config import XmlConfig
from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Directory, File, Execute
from resource_management.core import sudo


class KuduBase(Script):
    def __init__(self, role):
        self.role = role

    def install(self, env):
        self.configure(env)

    def check_master_started(self):
        '''ambari太奇葩了 没法保证不同组件启动顺序 只能自己写代码保证。
        通过访问master的web接口来确认master启动
        kudu master只要满足半数以上master是启动的，就可以提供正常服务
        '''
        import params
        import params_master
        total_master_num = len(params_master.kudu_master_hosts)
        alive_master_num = 0
        for h in params_master.kudu_master_hosts:
            url = 'http://%s:%s' % (h, params.master_webserver_port)
            try:
                r = requests.get(url)
                r.raise_for_status()
                alive_master_num = alive_master_num + 1
            except Exception as e:
                Logger.info('master not started: failed to reach %s: %s' % (url, e))
        if alive_master_num >= (total_master_num + 1) / 2:
            return True
        return False

    def check_tserver_started(self):
        '''
        通过访问tserver的web接口来确认tserver启动
        单机环境下tserver必须启动
        集群环境下至少需要三个tserver节点
        这样collector才能正常启动(需要创建表)
        '''
        import params
        import params_master
        config = Script.get_config()
        kudu_tserver_hosts = config['cluster_node_info']['kudu_tserver']['nodes']
        total_master_num = len(params_master.kudu_master_hosts)
        alive_tserver_num = 0
        for h in kudu_tserver_hosts:
            url = 'http://%s:%s' % (h, params.tserver_webserver_port)
            try:
                r = requests.get(url)
                r.raise_for_status()
                alive_tserver_num = alive_tserver_num + 1
            except Exception as e:
                Logger.info('tserver not started: failed to reach %s: %s' % (url, e))
        base_tserver_num = 3 if total_master_num > 1 else 1;
        if alive_tserver_num >= base_tserver_num:
            return True
        return False

    def _pid_file(self):
        # 之前的pid是kudu-tserver-kudu.pid 这样写的，直接适配
        import params
        return os.path.join(params.kudu_pid_dir, 'kudu-%s-kudu.pid' % self.role)

    def _flag_file(self):
        import params
        return os.path.join(params.kudu_conf_dir, '%s.gflagfile' % self.role)

    def start(self, env):
        # 这里需要重新生成配置文件是为了让通过命令修改的配置可以下发到各机器并重启生效
        self.configure(env)
        import params
        if self.role == self.KUDU_COLLECTOR:
            exec_path = os.path.join(params.collector_bin, 'kudu-%s' % self.role)
            cmd = 'echo $BASHPID > {pid} && exec {exec_path}  --flagfile={flag_file} &'.format(
                pid=self._pid_file(), exec_path=exec_path, flag_file=self._flag_file())
        elif self.role in [self.KUDU_MASTER, self.KUDU_TSERVER]:
            dump_info_path = os.path.join(params.kudu_conf_dir, 'kudu_%s.json' % self.role)
            bin = params.master_bin if self.role == self.KUDU_MASTER else params.tserver_bin
            exec_path = os.path.join(bin, 'kudu-%s' % self.role)
            cmd = 'echo $BASHPID > {pid} && exec {exec_path} --server_dump_info_path={dump_info_path} --flagfile={flag_file} &'.format(
                pid=self._pid_file(), exec_path=exec_path, dump_info_path=dump_info_path, flag_file=self._flag_file())
        else:
            raise Exception('unexpected role: [%s]' % self.role)
        Logger.info("start cmd: %s" % cmd)
        try:
            Execute(cmd, user=params.kudu_user)
        except Exception:
            show_logs(params.log_dir, params.kudu_user)
            raise

    def stop(self, env):
        """根据pid文件获取pid 然后发送kill命令"""
        import params
        pid_file = self._pid_file()
        check_process_status(pid_file, self.get_port())
        pid = int(sudo.read_file(pid_file))
        Logger.info("start kill %s" % pid)
        sudo.kill(pid, signal.SIGTERM)
        Logger.info("kill %s succeed" % pid)
        wait_process_stopped(pid_file)

    KUDU_MASTER = 'master'
    KUDU_TSERVER = 'tserver'
    KUDU_COLLECTOR = 'collector'

    def configure(self, env):
        import params
        env.set_params(params)
        for d in [params.log_dir, params.kudu_pid_dir, params.kudu_conf_dir]:
            Directory(
                d,
                mode=0o755,
                create_parents=True,
                owner=params.kudu_user,
                group=params.kudu_user_group)
        if self.role == self.KUDU_MASTER:
            import params_master
            env.set_params(params_master)
            dirs = [params_master.master_fs_wal_dir, params_master.master_log_dir, params_master.master_fs_data_dirs.split(',')]
            flag_kv = params_master.master_flag_config
            """如果开启认证需要配置"""
            if params.enable_ranger:
                import params_ranger_kudu_security
                XmlConfig(
                    params.ranger_security_file_name,
                    conf_dir=params.kudu_conf_dir,
                    configurations=params_ranger_kudu_security.ranger_kudu_security_config,
                    owner=params.kudu_user,
                    group=params.kudu_user_group,
                    mode=0o644)
                import params_ranger_kudu_audit
                XmlConfig(
                    params.ranger_audit_file_name,
                    conf_dir=params.kudu_conf_dir,
                    configurations=params_ranger_kudu_audit.ranger_kudu_audit_config,
                    owner=params.kudu_user,
                    group=params.kudu_user_group,
                    mode=0o644)
                import params_ranger_kudu_policymgr_ssl
                XmlConfig(
                    params.ranger_policymgr_file_name,
                    conf_dir=params.kudu_conf_dir,
                    configurations=params_ranger_kudu_policymgr_ssl.ranger_kudu_policymgr_config,
                    owner=params.kudu_user,
                    group=params.kudu_user_group,
                    mode=0o644)
                import params_core_site
                XmlConfig(
                    "core-site.xml",
                    conf_dir=params.kudu_conf_dir,
                    configurations=params_core_site.core_site_config,
                    owner=params.kudu_user,
                    group=params.kudu_user_group,
                    mode=0o644)
        elif self.role == self.KUDU_TSERVER:
            import params_tserver
            env.set_params(params_tserver)
            dirs = [params_tserver.tserver_fs_wal_dir, params_tserver.tserver_log_dir, params_tserver.tserver_fs_data_dirs.split(',')]
            flag_kv = params_tserver.tserver_flag_config
        elif self.role == self.KUDU_COLLECTOR:
            import params_collector
            env.set_params(params_collector)
            # collector 只需要存log、不需要写数据
            dirs = [params_collector.collector_log_dir]
            flag_kv = params_collector.collector_flag_config
        for d in dirs:
            Directory(
                d,
                mode=0o755,
                create_parents=True,
                owner=params.kudu_user,
                group=params.kudu_user_group)
        # modify gflagfile
        lines = []
        for k, v in flag_kv.items():
            lines.append('--%s=%s' % (k, v))
        content = '\n'.join(lines)
        File(
            self._flag_file(),
            content=content,
            mode=0o644,
            owner=params.kudu_user,
            group=params.kudu_user_group
        )

    def get_pid_files(self):
        return [self._pid_file()]

    def _get_role_pid(self):
        pid_name = 'kudu-%s' % self.role
        cmd = 'pidof %s' % pid_name
        from resource_management.core import shell
        ret, output = shell.checked_call(cmd)
        if ret != 0:
            raise Exception("Get pid for %s error." % pid_name)
        return output

    def status(self, env):
        check_process_status(self._pid_file(), self.get_port())

    def get_port(self):
        import params
        port = 0
        if self.role == self.KUDU_MASTER:
            port = params.master_service_port
        elif self.role == self.KUDU_TSERVER:
            port = params.tserver_service_port
        elif self.role == self.KUDU_COLLECTOR:
            port = params.collector_webserver_port
        else:
            Logger.info("Wrong kudu role!!!")
            raise ComponentIsNotRunning()
        return port
