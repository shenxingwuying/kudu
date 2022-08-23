#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import requests
import signal
import subprocess

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.libraries.functions.check_process_status import wait_process_stopped
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
            cmd = '{exec_path}  --flagfile={flag_file} 2>&1 &'.format(exec_path=exec_path, flag_file=self._flag_file())
        elif self.role in [self.KUDU_MASTER, self.KUDU_TSERVER]:
            dump_info_path = os.path.join(params.kudu_conf_dir, 'kudu_%s.json' % self.role)
            bin = params.master_bin if self.role == self.KUDU_MASTER else params.tserver_bin
            exec_path = os.path.join(bin, 'kudu-%s' % self.role)
            cmd = '{exec_path} --server_dump_info_path={dump_info_path} --flagfile={flag_file} 2>&1 &'.format(
                exec_path=exec_path, dump_info_path=dump_info_path, flag_file=self._flag_file())
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
        pid_file = self._pid_file()
        self.check_process_status()
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
        return self.check_process_status()

    def check_process_status(self):
        if self.check_port_alive("localhost", int(self.get_port())):
            pid, name = self.get_pid_by_port(int(self.get_port()))
            module = "kudu-{role}".format(role=self.role)
            if name == module:
                # pidfile is not correct, fix pid
                should_rewrite = False
                if not os.path.exists(self._pid_file()):
                    should_rewrite = True
                else:
                    durability_pid = int(sudo.read_file(self._pid_file()))
                    if durability_pid != int(pid):
                        should_rewrite = True
                if should_rewrite:
                    sudo.create_file(self._pid_file(), pid)
                return
            else:
                Logger.info("port is occupied by other process!!!")
                raise ComponentIsNotRunning()
        # Maybe process is starting, so we should check again
        if not os.path.exists(self._pid_file()):
            Logger.info("no such pid file, kudu-{role}".format(role=self.role))
            raise ComponentIsNotRunning()
        durability_pid = sudo.read_file(self._pid_file()).strip()
        shell_cmd = "ps -A -o pid,cmd | awk '{if(NF>=2)print $1,$2}'"
        t_result = subprocess.Popen(shell_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = t_result.stdout.read().rstrip()
        is_alive = False
        for pid_line in result.split('\n'):
            pid = pid_line.split(' ')[0]
            name = os.path.basename(pid_line.split(' ')[1])
            module = "kudu-{role}".format(role=self.role)
            if durability_pid == pid and name == module:
                is_alive = True
                break
        if not is_alive:
            raise ComponentIsNotRunning()

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

    def check_port_alive(self, host="localhost", port=0):
        import socket
        is_alive = True
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((host, port))
        if result != 0:
            Logger.info("process kudu-{name} not running, port: {port}".format(name=self.role, port=int(port)))
            is_alive = False
        return is_alive

    def get_pid_by_port(self, port):
        cmd = "ss -p -o state listening sport eq :{port} | sed 1d | sed " \
              "'s/.*((//g;s/))//g;s/\"//g;s/pid=//g;s/,fd=.*//g;s/,/ /g'".format(port=port)
        t_result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = t_result.stdout.read().rstrip()
        name = result.split(' ')[0]
        pid = result.split(' ')[1]

        # another implements:
        # cmd = "sudo netstat -nap | grep :{port} | grep LISTEN | sed 's#.*LISTEN[[:space:]]*##g;s#/# #g'".
        # format(port=port)
        # t_result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # result = t_result.stdout.read().rstrip()
        # pid = result.split(' ')[0]
        # name = result.split(' ')[1]
        return pid, name
