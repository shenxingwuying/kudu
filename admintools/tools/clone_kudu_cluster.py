#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import getpass
import json
import os
import socket
import sys
import subprocess
import time
import uuid
import yaml
import logging
import multiprocessing

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from argparse import ArgumentParser
sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_utils import shell_utils
from hyperion_guidance.ssh_connector import SSHConnector

master_meta_uuid = '0' * 32
ksyncer_uuid = 'beef' * 8
logger = logging.getLogger()
_lock = multiprocessing.Lock()
_config_kudu_tool = ""
_tmp_kudu_tool_path = "/home/sa_cluster/kudu"
_kudu_tool_min_version = "1.14.2"
_user_sa = "sa_cluster"


def run_cmd(cmd):
    print('****going to run: "' + cmd + '"')
    result = shell_utils.run_cmd(cmd, logger.info)
    if result['ret'] != 0:
        print('execute %s error:%s' % (cmd, result))
        sys.exit(1)
    return result


def run_ssh_cmd_with_password(cmd, host, ssh_port, user, password):
    ssh_connector = SSHConnector(host, user, password, ssh_port)
    result = ssh_connector.run_cmd(cmd)
    if result['ret'] != 0:
        sys.exit(1)
    return result


def copy_dir_to_remote_with_password(host, ssh_port, user, password, local_dir, remote_dir):
    ssh_connector = SSHConnector(host, user, password, ssh_port)
    ssh_connector.copy_dir_from_local(local_dir, remote_dir)
    # 无返回结果，需要再次检查下文件是否存在
    result = ssh_connector.check_file_exists(remote_dir)
    if result is False:
        sys.exit(1)


def copy_file_to_remote_with_password(host, ssh_port, user, password, local_file, remote_file):
    ssh_connector = SSHConnector(host, user, password, ssh_port)
    ssh_connector.copy_from_local(local_file, remote_file)
    # 无返回结果，需要再次检查下文件是否存在
    result = ssh_connector.check_file_exists(remote_file)
    if result is False:
        sys.exit(1)


def check_cluster(cluster_masters_str):
    masters_list = cluster_masters_str.split(',')
    if len(masters_list) != 1 and len(masters_list) != 3:
        return False
    output = run_cmd('%s cluster ksck %s' % (_config_kudu_tool, cluster_masters_str))
    return output['ret'] == 0


def check_kudu_version(kudu_tool, version):
    cmd = "%s -version" % kudu_tool
    vers1 = version.split(".")
    result = run_cmd(cmd)
    vers2 = result['stdout']
    vers2 = vers2.split("\n")[0]
    vers2 = vers2.split(" ")[1]
    vers2 = vers2.split("-")[0]
    vers2 = vers2.split(".")
    if len(vers1) > len(vers2):
        return False
    for i in range(len(vers1)):
        if int(vers2[i]) < int(vers1[i]):
            return False
    return True


def check_ssh(hosts, ssh_port):
    for host in hosts:
        if not check_service_connected(host, ssh_port):
            return False
    return True


def get_cluster_leader_master(cluster_masters_str):
    output = run_cmd('%s master list %s -columns=rpc-addresses,role -format=json' % (_config_kudu_tool, cluster_masters_str))
    masters = json.loads(output['stdout'])
    for master in masters:
        if master['role'] == 'LEADER':
            return master['rpc-addresses']
    return None


def get_cluster_tserver_list(cluster_masters_str):
    tserver_list = []
    output = run_cmd('%s tserver list %s -columns=rpc-addresses,uuid -format=json' % (_config_kudu_tool, cluster_masters_str))
    tservers = json.loads(output['stdout'])
    for tserver in tservers:
        if not tserver['uuid'] == ksyncer_uuid:
            tserver_list.append(tserver['rpc-addresses'])

    if len(tserver_list) == 0:
        print('Cluster(%s) has 0 tservers' % cluster_masters_str)
        sys.exit(1)

    return tserver_list


def get_server_config(role, rpc_addr, config):
    assert (role == 'master' or role == 'tserver')
    output = run_cmd('%s %s get_flags %s -flags=%s -format=json' % (_config_kudu_tool, role, rpc_addr, config))
    configs = json.loads(output['stdout'])
    assert (len(configs) == 1)
    assert ('value' in configs[0])
    return configs[0]['value']


def backup_dirs(host, ssh_port, wal_dir, data_dirs, timestamp, pwd):
    run_ssh_cmd_with_password("if [ -d %s ]; then sudo mv %s %s.bak.%s; fi" % (wal_dir, wal_dir, wal_dir, timestamp), host, ssh_port, _user_sa, pwd[host])
    for data_dir in data_dirs:
        run_ssh_cmd_with_password("if [ -d %s ]; then sudo mv %s %s.bak.%s; fi" % (data_dir, data_dir, data_dir, timestamp), host, ssh_port, _user_sa, pwd[host])


def chown_dirs(host, ssh_port, wal_dir, data_dirs, owner, pwd):
    run_ssh_cmd_with_password('sudo chown -R %s `dirname %s`' % (owner, wal_dir), host, ssh_port, _user_sa, pwd[host])
    for data_dir in data_dirs:
        run_ssh_cmd_with_password('sudo chown -R %s `dirname %s`' % (owner, data_dir), host, ssh_port, _user_sa, pwd[host])


def check_service_connected(host, port):
    n = 0
    while n < 3:
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)
        try:
            sk.connect((host, port))
            return True
        except Exception as e:
            print("%s:%s is closed. Exception: %s" % (host, port, e))
            n += 1
        sk.close()
    return False


def check_all_service_down(host_port_list):
    for host_port in host_port_list:
        host = host_port.split(':')[0]
        port = int(host_port.split(':')[1])
        if check_service_connected(host, port):
            return False
    return True


def generate_data_dir(base, timestamp):
    tmp_path = base + "_" + timestamp
    if os.path.exists(tmp_path):
        os.rmdir(tmp_path)
    os.mkdir(tmp_path)
    os.chdir(tmp_path)
    return os.getcwd()


def generate_master_data(old_master_leader,
                         index, new_master_uuids, new_master_peers,
                         fs_wal_dir, fs_data_dirs):
    run_cmd('%s fs format '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s '
            '-uuid=%s'
            % (_config_kudu_tool, fs_wal_dir, fs_data_dirs, new_master_uuids[index]))
    run_cmd('%s local_replica copy_from_remote %s %s '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s'
            % (_config_kudu_tool, master_meta_uuid, old_master_leader, fs_wal_dir, fs_data_dirs))
    run_cmd('%s local_replica cmeta rewrite_raft_config %s %s '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s'
            % (_config_kudu_tool, master_meta_uuid, new_master_peers, fs_wal_dir, fs_data_dirs))


def die(msg=''):
    """End the process, optionally after printing the passed error message."""
    print('ERROR: %s' % msg)
    sys.exit(1)


def parse_args():
    """Parse command line arguments and perform sanity checks."""
    parser = ArgumentParser()
    parser.add_argument('-l', '--local_source_cluster', required=True,
                        help='The source Kudu cluster master list, must be reachable and in heath status')
    parser.add_argument('-d', '--dest_cluster', required=True,
                        help='The destination Kudu cluster master list, must be reachable and in heath status')
    parser.add_argument('-s', '--ssh_port', type=int, default=22,
                        help='The destination Kudu cluster server ssh port')
    parser.add_argument('-p', '--parallel', type=int, default=4,
                        help='Parallel threads to clone a tserver, must be in range [1, 16]')
    parser.add_argument('-t', '--tablet_per_task', type=int, default=1,
                        help='The number of tablets cloned by one task')
    parser.add_argument('-k', '--kudu_tool_path', default='/sensorsdata/main/program/soku/soku_tool/kudu',
                        help='Path of the Kudu CLI tool, should support leader replica only clone')
    parser.add_argument('--table', default='',
                        help='table needed to be migrated to new cluster')
    parser.add_argument('--passwords', default='',
                        help='passwords of new cluster nodes, format:<{"hostname1":"pwd", "hostname2":"pwd"}')
    parser.add_argument('--ignore_dest_cluster_ksck', type=bool, default=False,
                        help='Not to check dest cluster health status')
    parser.add_argument('--continue_mode', type=bool, default=False,
                        help="""Whether to continue previous work. If --continue_mode is True,
                        --ignore_dest_cluster_ksck must be True too.""")
    args = parser.parse_args()
    print("倒计时3秒，在开始执行前，请确保目标集群和源集群主机的域名互相可识别，参见配置/etc/hosts")
    time.sleep(3)
    # Post processing checks
    print("0.1----检查目标kudu tool是否存在")
    if not os.path.isfile(args.kudu_tool_path):
        parser.print_usage()
        die('Kudu CLI tool is not exist')
    print("0.2----检查目标kudu tool版本是1.14.2以上")
    if not check_kudu_version(args.kudu_tool_path, _kudu_tool_min_version):
        die('Kudu CLI tool should be >= 1.14.2')
    global _config_kudu_tool
    _config_kudu_tool = args.kudu_tool_path
    if len(args.table) <= 0:
        print("parameter --table is not configed, will migrate the whole cluster")
    else:
        if "*" in args.table:
            die("Parameter: --table can not contains *")
        print("parameter --table is configed, will migrate table:%s" % args.table)
    if len(args.table) <= 0 and len(args.passwords) <= 0:
        die("parameter --passwords is not configed")
    print("0.3----检查本地kudu集群是否正常")
    if not check_cluster(args.local_source_cluster):
        parser.print_usage()
        die('Source cluster must be reachable and in heath status')
    print("0.4----检查目标kudu集群是否正常")
    if not args.ignore_dest_cluster_ksck and not check_cluster(args.dest_cluster):
        parser.print_usage()
        die('Destination cluster must be reachable and in heath status')
    if args.parallel < 1 or args.parallel > 16:
        parser.print_usage()
        die('Parallel must be in range [1, 16]')
    if args.tablet_per_task < 1:
        parser.print_usage()
        die('tablet_per_task must be larger than 0]')
    if args.continue_mode and not args.ignore_dest_cluster_ksck:
        parser.print_usage()
        die('If --continue_mode is True, --ignore_dest_cluster_ksck must be True too')
    return args


def set_progress_stage_1(new_master_fs_wal_dir, new_master_fs_data_dirs_list,
                         new_tserver_list, new_tserver_fs_wal_dir,
                         new_tserver_fs_data_dirs, pwd, host_uuid_map):
    progress = {'stage': 1, 'dest_cluster': {}, 'password': {}, 'uuids': {}}
    progress['dest_cluster']['master'] = {}
    progress['dest_cluster']['master']['fs_wal_dir'] = new_master_fs_wal_dir
    progress['dest_cluster']['master']['fs_data_dirs'] = new_master_fs_data_dirs_list
    progress['dest_cluster']['tserver'] = {}
    progress['dest_cluster']['tserver']['nodes'] = new_tserver_list
    progress['dest_cluster']['tserver']['fs_wal_dir'] = new_tserver_fs_wal_dir
    progress['dest_cluster']['tserver']['fs_data_dirs'] = new_tserver_fs_data_dirs
    for key in pwd:
        progress['password'][key] = pwd[key]
    for key in host_uuid_map:
        progress['uuids'][key] = host_uuid_map[key]
    return progress


def set_progress_stage_1plus(progress, stage):
    progress['stage'] = stage
    return progress


def set_progress_stage_clone_stage(progress, clone_stage, task_name):
    progress['task'][task_name]['clone_stage'] = clone_stage
    return progress


def migrate_table(progress, args, progress_file):
    stage1_time = int(time.time())
    if progress['stage'] < 2:
        # 检查目标集群是否已经存在该表
        cmd = "%s table list %s -tables=%s " % (_config_kudu_tool, args.dest_cluster, args.table)
        result = run_cmd(cmd)
        if len(result['stdout']) > 0:
            die("Table:%s exists in destination cluster, should delete it" % args.table)
        # 检查本地集群是否有该表
        cmd = "%s table list %s -tables=%s " % (_config_kudu_tool, args.local_source_cluster, args.table)
        result = run_cmd(cmd)
        if len(result['stdout']) <= 0:
            die("Table:%s not exists in local cluster" % args.table)

        cmd = "%s table copy %s %s %s -num_threads=%s -write_type=upsert -create_table=true" % \
            (_config_kudu_tool, args.local_source_cluster, args.table, args.dest_cluster, args.parallel)
        run_cmd(cmd)
        progress = set_progress_stage_1plus(progress, 2)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 2')
    stage2_time = int(time.time())
    print("Finish stage 2, cost time: %s" % (stage2_time - stage1_time))
    print("Finish table migrate task!")


def main():
    start_time = int(time.time())
    if getpass.getuser() != _user_sa:
        print('请在sa_cluster用户下执行本工具！')
        sys.exit(1)
    args = parse_args()
    root_path = os.getcwd()
    # 加载进度文件
    progress = {'stage': 0}
    progress_file = root_path + '/progress_file.yaml'
    if args.continue_mode:
        if not os.path.isfile(progress_file):
            die('If --continue_mode is True, progress_file.yaml must exist in current directory.')
        with open(progress_file, 'r') as f:
            progress = yaml.load(f)
            if not progress:
                die('progress_file.yaml is error.')

    # 单表迁移
    if len(args.table) > 0:
        migrate_table(progress, args, progress_file)
        return

    # stage: 1, 生成目标集群配置
    print("Begin stage 1: 生成目标集群配置")
    # 目标集群masters
    new_masters_list = args.dest_cluster.split(',')
    new_master_hosts = list(map(lambda master: master.split(':')[0], new_masters_list))
    if not progress or 'stage' not in progress or progress['stage'] < 1:
        # 请确保其父目录是 kudu:sa_group 权限！！！
        print("1.1----获取目标集群master_fs_wal_dir")
        new_master_fs_wal_dir = get_server_config('master', new_masters_list[0], 'fs_wal_dir')

        # 请确保其父目录是 kudu:sa_group 权限！！！
        print("1.2----获取目标集群master_fs_data_dirs")
        new_master_fs_data_dirs_list = get_server_config('master', new_masters_list[0], 'fs_data_dirs').split(',')
        if len(new_master_fs_data_dirs_list) != 1:
            die('Dest master should have only 1 dir for config fs_data_dirs')

        # 目标集群tservers
        print("1.3----获取目标集群tserver地址")
        new_tserver_list = get_cluster_tserver_list(args.dest_cluster)
        new_tserver_hosts = list(map(lambda tserver: tserver.split(':')[0], new_tserver_list))
        new_1st_tserver = new_tserver_list[0]

        # 检查ssh连通性
        print("1.4----检查目标集群master tserver连通性")
        if not check_ssh(new_master_hosts + new_tserver_hosts, args.ssh_port):
            die('Some host could not ssh')

        # 请确保其父目录是 kudu:sa_group 权限！！！
        print("1.5----获取目标集群tserver_fs_wal_dir")
        new_tserver_fs_wal_dir = get_server_config('tserver', new_1st_tserver, 'fs_wal_dir')

        # 请确保其父目录是 kudu:sa_group 权限！！！
        print("1.6----获取目标集群tserver_fs_data_dirs")
        new_tserver_fs_data_dirs = get_server_config('tserver', new_1st_tserver, 'fs_data_dirs').split(',')

        # 请确保其父目录是 kudu:sa_group 权限！！！
        if len(args.passwords) <= 0:
            print("1.7----获取目标集群master/tserver sa_cluster密码，密码存在/home/sa_cluster/.sa_password中")
            pwd = {}
            for host in new_master_hosts:
                print("请输入主机:%s sa_cluster账号的密码" % host)
                result = run_cmd("ssh %s@%s -p %d cat ~/.sa_password" % (_user_sa, host, args.ssh_port))
                pwd[host] = result['stdout']
            for host in new_tserver_hosts:
                if host in pwd:
                    continue
                print("请输入主机:%s sa_cluster账号的密码" % host)
                result = run_cmd("ssh %s@%s -p %d cat ~/.sa_password" % (_user_sa, host, args.ssh_port))
                pwd[host] = result['stdout']
        else:
            print("1.7----解析命令行中passwords参数")
            try:
                pwd = json.loads(args.passwords)
            except Exception:
                print("can't parse parameter --passwords, it should be in json format")
                return

        # 为目标集群TServer生成uuid
        host_uuid_map = gen_uuids(new_tserver_hosts)

        # 更新进度
        progress = set_progress_stage_1(new_master_fs_wal_dir, new_master_fs_data_dirs_list,
                                        new_tserver_list, new_tserver_fs_wal_dir,
                                        new_tserver_fs_data_dirs, pwd, host_uuid_map)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 1')
    stage1_time = int(time.time())
    print("Finish stage 1, cost time: %s" % (stage1_time - start_time))

    print("Begin stage 2: 生成目标集群master数据")
    # stage: 2, 生成目标集群master数据
    timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime())
    new_master_fs_wal_dir = progress['dest_cluster']['master']['fs_wal_dir']
    new_master_fs_data_dirs_list = progress['dest_cluster']['master']['fs_data_dirs']
    # 目标集群tservers
    new_tserver_list = progress['dest_cluster']['tserver']['nodes']
    new_tserver_hosts = list(map(lambda tserver: tserver.split(':')[0], new_tserver_list))
    new_1st_tserver = new_tserver_list[0]
    pwd = progress['password']
    if progress['stage'] < 2:
        # 等待目标集群停止
        while True:
            print("2.1----开始停止目标集群(%s) Kudu服务！" % args.dest_cluster)
            run_ssh_cmd_with_password('spadmin mothership stop -m kudu', new_master_hosts[0], args.ssh_port, _user_sa, pwd[new_master_hosts[0]])
            if check_all_service_down(new_masters_list + new_tserver_list):
                break
            time.sleep(10)

        # 备份目标集群master
        print("2.2----开始备份目标集群master wal_dir/data_dir数据")
        for new_master_host in new_master_hosts:
            backup_dirs(new_master_host, args.ssh_port, new_master_fs_wal_dir, new_master_fs_data_dirs_list, timestamp, pwd)

        # 为新集群生成新的master uuid
        print("2.3----为新集群生成新的master uuid")
        new_master_uuids = []
        new_master_peers = ''
        for new_master in new_masters_list:
            new_master_uuid = uuid.uuid4().hex
            new_master_uuids.append(new_master_uuid)
            new_master_peers += ('%s:%s ' % (new_master_uuid, new_master))

        # 生成master数据，并拷贝到目标集群
        print("2.4----生成master数据，并拷贝到目标集群")
        i = 0
        old_master_leader = get_cluster_leader_master(args.local_source_cluster)
        for new_master in new_masters_list:
            os.chdir(root_path)
            new_masters_host = new_master.split(':')[0]

            # 创建临时目录
            print("2.4.1--------为目标集群创建临时目录")
            cwd = generate_data_dir(new_masters_host, timestamp)
            fs_wal_dir = '%s/master_wal' % cwd
            fs_data_dirs = '%s/master_data' % cwd

            # 生成master数据
            print("2.4.2--------准备原始数据")
            generate_master_data(old_master_leader,
                                 i, new_master_uuids, new_master_peers,
                                 fs_wal_dir, fs_data_dirs)

            # 拷贝master数据
            print("2.4.3--------开始拷贝master数据")
            chown_dirs(new_masters_host, args.ssh_port,
                       new_master_fs_wal_dir, new_master_fs_data_dirs_list, 'sa_cluster:sa_group', pwd)
            copy_dir_to_remote_with_password(new_masters_host, args.ssh_port, _user_sa, pwd[new_masters_host], '%s/' % fs_wal_dir, new_master_fs_wal_dir)
            copy_dir_to_remote_with_password(new_masters_host, args.ssh_port, _user_sa, pwd[new_masters_host], '%s/' % fs_data_dirs, new_master_fs_data_dirs_list[0])
            chown_dirs(new_masters_host, args.ssh_port,
                       new_master_fs_wal_dir, new_master_fs_data_dirs_list, 'kudu:sa_group', pwd)

            i += 1
        progress = set_progress_stage_1plus(progress, 2)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 2')
    stage2_time = int(time.time())
    print("Finish stage 2, cost time: %s" % (stage2_time - stage1_time))

    # stage: 3, 生成目标集群tserver目录
    print("Begin stage: 3, 生成目标集群tserver目录")
    new_tserver_fs_wal_dir = progress['dest_cluster']['tserver']['fs_wal_dir']
    new_tserver_fs_data_dirs = progress['dest_cluster']['tserver']['fs_data_dirs']
    if progress['stage'] < 3:
        # 备份目标集群tserver
        print("3.1----备份目标集群tserver wal_dir/data_dirs数据")
        for new_tserver_host in new_tserver_hosts:
            backup_dirs(new_tserver_host, args.ssh_port,
                        new_tserver_fs_wal_dir, new_tserver_fs_data_dirs, timestamp, pwd)

        # 创建目标集群的tserver目录
        print("3.2-----创建目标集群的tserver目录")
        for new_tserver_host in new_tserver_hosts:
            cmd = 'sudo sp_kudu fs format -fs_data_dirs=%s -fs_wal_dir=%s -uuid=%s' % (','.join(new_tserver_fs_data_dirs), new_tserver_fs_wal_dir, host_uuid_map[new_tserver_host])
            run_ssh_cmd_with_password(cmd, new_tserver_host, args.ssh_port, _user_sa, pwd[new_tserver_host])
            chown_dirs(new_tserver_host, args.ssh_port,
                       new_tserver_fs_wal_dir, new_tserver_fs_data_dirs, 'kudu:sa_group', pwd)
        progress = set_progress_stage_1plus(progress, 3)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 3')
    stage3_time = int(time.time())
    print("Finish stage 3, cost time:%s" % (stage3_time - stage2_time))

    # stage: 4, 拷贝kudu tool到目标tserver并修改权限
    print("Begin stage 4: 拷贝kudu tool到目标tserver并修改权限")
    print("获取需要拷贝的所有leader tablet和它所在的tserver地址")
    ts_tablet_map = get_leader_tablet_and_addr(args.local_source_cluster)
    if progress['stage'] < 4:
        for tserver_host in new_tserver_hosts:
            cmd = 'if [[ ! -z {tool} ]] && [ -f {tool} ]; then sudo mv {tool} {tool}.bak; fi'.format(tool=_tmp_kudu_tool_path)
            run_ssh_cmd_with_password(cmd, tserver_host, args.ssh_port, _user_sa, pwd[tserver_host])
            copy_file_to_remote_with_password(tserver_host, args.ssh_port, _user_sa, pwd[tserver_host], _config_kudu_tool, _tmp_kudu_tool_path)
            # 修改kudu tool权限
            run_ssh_cmd_with_password('sudo chown kudu:sa_group %s' % _tmp_kudu_tool_path, tserver_host, args.ssh_port, _user_sa, pwd[tserver_host])
        progress = set_progress_stage_1plus(progress, 4)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 4')
    stage4_time = int(time.time())
    print("Finish stage 4, cost time:%s" % (stage4_time - stage3_time))

    # stage: 5, tablet为粒度生成拷贝任务
    # 一台tserver只能跑一个kudu clone进程，否则会出现锁竞争问题
    # 算法：
    #     1. 首先获取旧集群每台tserver上的所有leader tablet，例如m个tablet
    #     2. 根据参数t，将m个tablet划分成m/t个任务
    #     3. 假如新集群有n台tserver，则每台tserver启动一个线程，总共n个线程消费这些task
    #     4. 只用记录每个task的进度，task进度为0表示没有消费，为3则表示消费完成
    print("Begin stage: 5, tablet为粒度生成拷贝任务")
    if progress['stage'] < 5:
        # 分配拷贝任务
        os.chdir(root_path)
        task_id = 0
        progress['task'] = {}
        # 为每台tserver生成task. from_tserver参数限制，只能以tserver纬度拷贝tablet
        for tserver in ts_tablet_map:
            all_tablets = ts_tablet_map[tserver]
            step = args.tablet_per_task
            seperator = ","
            i = 0
            # 根据step生成task
            while i + step < len(all_tablets):
                tablets_str = seperator.join(all_tablets[i:i + step])
                task_name = 'task_%s' % task_id
                progress['task'][task_name] = {}
                progress['task'][task_name]['kudu_tool_path'] = _config_kudu_tool
                progress['task'][task_name]['ssh_port'] = args.ssh_port
                progress['task'][task_name]['from_tserver'] = tserver
                progress['task'][task_name]['tablet_id'] = tablets_str
                progress['task'][task_name]['from_master'] = args.local_source_cluster
                progress['task'][task_name]['to_tserver_fs_data_dirs'] = new_tserver_fs_data_dirs
                progress['task'][task_name]['to_tserver_fs_wal_dir'] = new_tserver_fs_wal_dir
                progress['task'][task_name]['parallel'] = args.parallel
                progress['task'][task_name]['clone_stage'] = 0
                task_id += 1
                i += step
            # 不足一个step的，则单独作为一个task
            if i < len(all_tablets):
                tablets_str = seperator.join(all_tablets[i:len(all_tablets)])
                task_name = 'task_%s' % task_id
                progress['task'][task_name] = {}
                progress['task'][task_name]['kudu_tool_path'] = _config_kudu_tool
                progress['task'][task_name]['ssh_port'] = args.ssh_port
                progress['task'][task_name]['from_tserver'] = tserver
                progress['task'][task_name]['tablet_id'] = tablets_str
                progress['task'][task_name]['from_master'] = args.local_source_cluster
                progress['task'][task_name]['to_tserver_fs_data_dirs'] = new_tserver_fs_data_dirs
                progress['task'][task_name]['to_tserver_fs_wal_dir'] = new_tserver_fs_wal_dir
                progress['task'][task_name]['parallel'] = args.parallel
                progress['task'][task_name]['clone_stage'] = 0
                task_id += 1
            with open(progress_file, 'w+') as f:
                yaml.dump(progress, f)
        # 更新进度
        progress = set_progress_stage_1plus(progress, 5)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 5')
    stage5_time = int(time.time())
    print("Finish stage 5, cost time: %s" % (stage5_time - stage4_time))

    # stage: 6, 多进程执行拷贝命令
    # 开启一个线程池，线程个数等于新集群tserver个数n
    # tserver上不能同时执行kudu clone，进程不安全
    # 每次提交n个task，并且生成n种类型的task，一种类型的task只能去固定的tserver执行
    # 某种类型的task消费完了，立即生成对应的类型的task，提交到线程池
    # 这样保证任何时刻，一台tserver上只有一个task在执行，不存在线程安全问题
    # 只需记录的task是否执行完成
    print("Begin stage: 6, 多进程执行拷贝命令")
    if progress['stage'] < 6:
        # 创建线程池
        max_threads = len(new_tserver_list)
        executor = ProcessPoolExecutor(max_workers=max_threads)
        # 获取所有task
        task_num = len(progress['task'])
        # 初始生产min(max_threads, task_num)个task并投喂
        task_index = 0
        tasks = {}
        max_concurrent = min(max_threads, task_num)
        while task_index < max_concurrent:
            task_name = "task_%s" % task_index
            task = progress['task'][task_name]
            # 补充to_tserver信息
            to_tserver = new_tserver_list[task_index].split(':')
            task['to_tserver'] = to_tserver[0]
            task['to_tserver_port'] = to_tserver[1]
            tasks[task_index] = executor.submit(clone_data, task, task_name, progress_file, progress)
            task_index += 1
        # 不断判断task是否执行完成，完成的话，则再次提交该类型的task
        # 退出条件是所有task都被消费完
        while True:
            i = 0
            while i < max_concurrent:
                if tasks[i].done():
                    # 所有task都被消费完成
                    if task_index >= task_num:
                        break
                    task_name = "task_%s" % task_index
                    task = progress['task'][task_name]
                    # 补充to_tserver信息
                    to_tserver = new_tserver_list[i].split(':')
                    task['to_tserver'] = to_tserver[0]
                    task['to_tserver_port'] = to_tserver[1]
                    # 替换原来已经执行完成的task
                    print("submit task:%s" % task_name)
                    tasks[i] = executor.submit(clone_data, task, task_name, progress_file, progress)
                    task_index += 1
                i += 1
            if task_index >= task_num:
                break
        # 等待线程池中所有task完成
        executor.shutdown(wait=True)
        # 如果所有任务都完成了，则更新进度
        with open(progress_file, 'r') as f:
            progress = yaml.load(f)
            if not progress:
                die('progress_file.yaml is error.')
        all_task_finished_flag = True
        for task_name in progress['task']:
            if progress['task'][task_name]['clone_stage'] != 4:
                all_task_finished_flag = False
                break
        if all_task_finished_flag:
            progress = set_progress_stage_1plus(progress, 6)
            with open(progress_file, 'w+') as f:
                yaml.dump(progress, f)
        else:
            die("Some task executes failed!")
    else:
        print('Skip stage 6')
    stage6_time = int(time.time())
    print("Finish stage 6, cost time: %s" % (stage6_time - stage5_time))

    print('已完成,总耗时：%s, 请手动启动目标集群' % (stage6_time - start_time))


def ksck(master_addr, sections="TABLET_SUMMARIES"):
    ksck_cmd = '%s cluster ksck %s -ksck_format=json_compact -sections=%s' % (_config_kudu_tool, master_addr, sections)
    # 无法使用云平台的接口，因为含有特殊字符，云平台的接口解析不了
    p = subprocess.Popen([ksck_cmd], stdout=subprocess.PIPE, shell=True)
    stdout, _ = p.communicate()
    stdout = stdout.strip()
    if p.returncode != 0:
        die("execute command:%s failed" % ksck_cmd)
    # ksck json格式输出，partitioin key包含一个特殊编码，需要解析成cp1252
    stdout = stdout.decode("cp1252")
    return stdout


def get_leader_tablet_and_addr(master_addr):
    ksck_output = ksck(master_addr)
    tablets = json.loads(ksck_output)
    ts_tablet_map = defaultdict(list)
    for tablet in tablets['tablet_summaries']:
        tablet_id = tablet['id']
        replicas = tablet['replicas']
        for replica in replicas:
            if replica['is_leader'] is True:
                ts_tablet_map[replica['ts_address']].append(tablet_id)
    return ts_tablet_map


def clone_data(task, task_name, progress_file, progress):
    ssh_port = task['ssh_port']
    from_tserver = task['from_tserver']
    to_tserver_host = task['to_tserver']
    to_tserver_port = task['to_tserver_port']
    tablet_ids = task['tablet_id']
    to_tserver_fs_data_dirs = task['to_tserver_fs_data_dirs']
    to_tserver_fs_wal_dir = task['to_tserver_fs_wal_dir']
    parallel = task['parallel']
    pwd = progress['password']
    host_uuid_map = progress['uuids']
    kudu_tool = _tmp_kudu_tool_path
    # 1. clone tserver 数据
    print("task:%s clone tserver data, to_tserver:%s" % (task_name, to_tserver_host))
    if progress['task'][task_name]['clone_stage'] < 1:
        copy_from_remote(tablet_ids, from_tserver, ','.join(to_tserver_fs_data_dirs),
                         to_tserver_fs_wal_dir, to_tserver_host, pwd, ssh_port, parallel, kudu_tool)
        _lock.acquire()
        # 先读出来，再修改，再写进去
        with open(progress_file, 'r') as f:
            progress = yaml.load(f)
            if not progress:
                die('progress_file.yaml is error.')
        set_progress_stage_clone_stage(progress, 1, task_name)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
        _lock.release()
    else:
        print("task:%s skip clone stage 1" % task_name)

    # 2. rewrite Raft config
    if progress['task'][task_name]['clone_stage'] < 2:
        to_tserver_hp = "%s:%s" % (to_tserver_host, to_tserver_port)
        raft_peers = "%s:%s" % (host_uuid_map[to_tserver_host], to_tserver_hp)
        rewrite_raft_config(tablet_ids, raft_peers, ','.join(to_tserver_fs_data_dirs),
                            to_tserver_fs_wal_dir,
                            to_tserver_host, ssh_port, pwd, kudu_tool)
        _lock.acquire()
        # 先读出来，再修改，再写进去
        with open(progress_file, 'r') as f:
            progress = yaml.load(f)
            if not progress:
                die('progress_file.yaml is error.')
        set_progress_stage_clone_stage(progress, 2, task_name)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
        _lock.release()
    else:
        print("task:%s skip clone stage 2" % task_name)
    # 3. 修改wal_dir权限
    print("task:%s chown wal dir" % task_name)
    if progress['task'][task_name]['clone_stage'] < 3:
        cmd = 'sudo chown -R kudu:sa_group `dirname %s`' % to_tserver_fs_wal_dir
        run_ssh_cmd_with_password(cmd, to_tserver_host, ssh_port, _user_sa, pwd[to_tserver_host])
        _lock.acquire()
        # 先读出来，再修改，再写进去
        with open(progress_file, 'r') as f:
            progress = yaml.load(f)
            if not progress:
                die('progress_file.yaml is error.')
        set_progress_stage_clone_stage(progress, 3, task_name)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
        _lock.release()
    else:
        print("task:%s skip clone stage 3" % task_name)

    # 4. 修改data_dir权限
    print("task:%s chown data dir" % task_name)
    if progress['task'][task_name]['clone_stage'] < 4:
        cmd = 'sudo chown -R kudu:sa_group `dirname %s`' % ' '.join(to_tserver_fs_data_dirs)
        run_ssh_cmd_with_password(cmd, to_tserver_host, ssh_port, _user_sa, pwd[to_tserver_host])
        _lock.acquire()
        # 先读出来，再修改，再写进去
        with open(progress_file, 'r') as f:
            progress = yaml.load(f)
            if not progress:
                die('progress_file.yaml is error.')
        set_progress_stage_clone_stage(progress, 4, task_name)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
        _lock.release()
    else:
        print("task:%s skip clone stage 4" % task_name)
    return 0


def copy_from_remote(tablet_ids, from_tserver_hp, fs_data_dirs,
                     fs_wal_dir, to_tserver_host, pwd, ssh_port, parallel, kudu_tool):
    cmd = "%s local_replica copy_from_remote %s %s -fs_data_dirs=%s -fs_wal_dir=%s -num_max_threads=%s" \
          % (kudu_tool, tablet_ids, from_tserver_hp, fs_data_dirs, fs_wal_dir, parallel)
    cmd = "sudo sh -c '%s'" % cmd
    return run_ssh_cmd_with_password(cmd, to_tserver_host, ssh_port, _user_sa,
                                       pwd[to_tserver_host])


def rewrite_raft_config(tablet_ids, raft_peers, fs_data_dirs, fs_wal_dir,
                        to_tserver_host, ssh_port, pwd, kudu_tool):
    cmd = "%s local_replica cmeta rewrite_raft_config %s %s -fs_data_dirs=%s -fs_wal_dir=%s" % \
          (kudu_tool, tablet_ids, raft_peers, fs_data_dirs, fs_wal_dir)
    cmd = "sudo sh -c '%s'" % cmd
    return run_ssh_cmd_with_password(cmd, to_tserver_host, ssh_port, _user_sa,
                                     pwd[to_tserver_host])


def gen_uuids(hosts):
    host_uuid_map = {}
    for host in hosts:
        new_uuid = uuid.uuid4().hex
        host_uuid_map[host] = new_uuid
    return host_uuid_map


if __name__ == '__main__':
    print("stage 0: 开始执行脚本，进行前期的检查")
    exit(main())
