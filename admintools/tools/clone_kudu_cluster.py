#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import getpass
import json
import os
import socket
import sys
import time
import uuid
import yaml
import logging
import multiprocessing

from argparse import ArgumentParser
sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_utils import shell_utils
from hyperion_guidance.ssh_connector import SSHConnector

master_meta_uuid = '0' * 32
ksyncer_uuid = 'beef' * 8
logger = logging.getLogger()
_lock = multiprocessing.Lock()


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
    output = run_cmd('export PATH=$PWD:$PATH && kudu cluster ksck %s' % cluster_masters_str)
    return output['ret'] == 0


def check_kudu_version(version):
    cmd = "export PATH=$PWD:$PATH && kudu -version"
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
    output = run_cmd('export PATH=$PWD:$PATH && kudu master list %s -columns=rpc-addresses,role -format=json' % cluster_masters_str)
    masters = json.loads(output['stdout'])
    for master in masters:
        if master['role'] == 'LEADER':
            return master['rpc-addresses']
    return None


def get_cluster_tserver_list(cluster_masters_str):
    tserver_list = []
    output = run_cmd('export PATH=$PWD:$PATH && kudu tserver list %s -columns=rpc-addresses,uuid -format=json' % cluster_masters_str)
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
    output = run_cmd('export PATH=$PWD:$PATH && kudu %s get_flags %s -flags=%s -format=json' % (role, rpc_addr, config))
    configs = json.loads(output['stdout'])
    assert (len(configs) == 1)
    assert ('value' in configs[0])
    return configs[0]['value']


def backup_dirs(host, ssh_port, wal_dir, data_dirs, timestamp, pwd):
    run_ssh_cmd_with_password("if [ -d %s ]; then sudo mv %s %s.bak.%s; fi" % (wal_dir, wal_dir, wal_dir, timestamp), host, ssh_port, 'sa_cluster', pwd[host])
    for data_dir in data_dirs:
        run_ssh_cmd_with_password("if [ -d %s ]; then sudo mv %s %s.bak.%s; fi" % (data_dir, data_dir, data_dir, timestamp), host, ssh_port, 'sa_cluster', pwd[host])


def chown_dirs(host, ssh_port, wal_dir, data_dirs, owner, pwd):
    run_ssh_cmd_with_password('sudo chown -R %s `dirname %s`' % (owner, wal_dir), host, ssh_port, 'sa_cluster', pwd[host])
    for data_dir in data_dirs:
        run_ssh_cmd_with_password('sudo chown -R %s `dirname %s`' % (owner, data_dir), host, ssh_port, 'sa_cluster', pwd[host])


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
    run_cmd('export PATH=$PWD:$PATH && kudu fs format '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s '
            '-uuid=%s'
            % (fs_wal_dir, fs_data_dirs, new_master_uuids[index]))
    run_cmd('export PATH=$PWD:$PATH && kudu local_replica copy_from_remote %s %s '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s'
            % (master_meta_uuid, old_master_leader, fs_wal_dir, fs_data_dirs))
    run_cmd('export PATH=$PWD:$PATH && kudu local_replica cmeta rewrite_raft_config %s %s '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s'
            % (master_meta_uuid, new_master_peers, fs_wal_dir, fs_data_dirs))


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
    parser.add_argument('-t', '--thread_num', type=int, default=1,
                        help='Parallel threads to execute clone task, must be in range [1, 16]')
    parser.add_argument('-k', '--kudu_tool_path', default='/sensorsdata/main/program/soku/soku_tool/kudu',
                        help='Path of the Kudu CLI tool, should support leader replica only clone')
    parser.add_argument('--ignore_dest_cluster_ksck', type=bool, default=False,
                        help='Not to check dest cluster health status')
    parser.add_argument('--continue_mode', type=bool, default=False,
                        help="""Whether to continue previous work. If --continue_mode is True,
                        --ignore_dest_cluster_ksck must be True too.""")
    args = parser.parse_args()
    print("倒计时3秒，在开始执行前，请确保目标集群和源集群主机的域名互相可识别，参见配置/etc/hosts")
    time.sleep(3)
    # Post processing checks
    print("0.1----检查本地kudu集群是否正常")
    if not check_cluster(args.local_source_cluster):
        parser.print_usage()
        die('Source cluster must be reachable and in heath status')
    print("0.2----检查目标kudu集群是否正常")
    if not args.ignore_dest_cluster_ksck and not check_cluster(args.dest_cluster):
        parser.print_usage()
        die('Destination cluster must be reachable and in heath status')
    if args.parallel < 1 or args.parallel > 16:
        parser.print_usage()
        die('Parallel must be in range [1, 16]')
    if args.thread_num < 1 or args.thread_num > 16:
        parser.print_usage()
        die('Thread number must be in range [1, 16]')
    print("0.3----检查目标kudu tool是否存在")
    if not os.path.isfile(args.kudu_tool_path):
        parser.print_usage()
        die('Kudu CLI tool is not exist')
    print("0.4----检查目标kudu tool版本是1.14.2以上")
    if not check_kudu_version('1.14.2'):
        die('Kudu CLI tool should be >= 1.14.2')
    if args.continue_mode and not args.ignore_dest_cluster_ksck:
        parser.print_usage()
        die('If --continue_mode is True, --ignore_dest_cluster_ksck must be True too')
    return args


def set_progress_stage_1(new_master_fs_wal_dir, new_master_fs_data_dirs_list,
                         new_tserver_list, new_tserver_fs_wal_dir, new_tserver_fs_data_dirs, pwd):
    progress = {'stage': 1, 'dest_cluster': {}, 'password': {}}
    progress['dest_cluster']['master'] = {}
    progress['dest_cluster']['master']['fs_wal_dir'] = new_master_fs_wal_dir
    progress['dest_cluster']['master']['fs_data_dirs'] = new_master_fs_data_dirs_list
    progress['dest_cluster']['tserver'] = {}
    progress['dest_cluster']['tserver']['nodes'] = new_tserver_list
    progress['dest_cluster']['tserver']['fs_wal_dir'] = new_tserver_fs_wal_dir
    progress['dest_cluster']['tserver']['fs_data_dirs'] = new_tserver_fs_data_dirs
    for key in pwd:
        progress['password'][key] = pwd[key]
    return progress


def set_progress_stage_1plus(progress, stage):
    progress['stage'] = stage
    return progress


def set_progress_stage_clone_stage(progress, clone_stage, task_name):
    progress[task_name]['clone_stage'] = clone_stage
    return progress


def main():
    if getpass.getuser() != 'sa_cluster':
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
            progress = yaml.load(f, Loader=yaml.FullLoader)
            if not progress:
                die('progress_file.yaml is error.')

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
        print("1.7----获取目标集群master/tserver sa_cluster密码，密码存在/home/sa_cluster/.sa_password中")
        pwd = {}
        for host in new_master_hosts:
            print("请输入主机:%s sa_cluster账号的密码" % host)
            result = run_cmd("ssh sa_cluster@%s -p %d cat ~/.sa_password" % (host, args.ssh_port))
            pwd[host] = result['stdout']
        for host in new_tserver_hosts:
            if host in pwd:
                continue
            print("请输入主机:%s sa_cluster账号的密码" % host)
            result = run_cmd("ssh sa_cluster@%s -p %d cat ~/.sa_password" % (host, args.ssh_port))
            pwd[host] = result['stdout']
        # 更新进度
        progress = set_progress_stage_1(new_master_fs_wal_dir, new_master_fs_data_dirs_list,
                                        new_tserver_list, new_tserver_fs_wal_dir, new_tserver_fs_data_dirs, pwd)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 1')
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
            run_ssh_cmd_with_password('spadmin mothership stop -m kudu', new_master_hosts[0], args.ssh_port, "sa_cluster", pwd[new_master_hosts[0]])
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
            copy_dir_to_remote_with_password(new_masters_host, args.ssh_port, 'sa_cluster', pwd[new_masters_host], '%s/' % fs_wal_dir, new_master_fs_wal_dir)
            copy_dir_to_remote_with_password(new_masters_host, args.ssh_port, 'sa_cluster', pwd[new_masters_host], '%s/' % fs_data_dirs, new_master_fs_data_dirs_list[0])
            chown_dirs(new_masters_host, args.ssh_port,
                       new_master_fs_wal_dir, new_master_fs_data_dirs_list, 'kudu:sa_group', pwd)

            i += 1
        progress = set_progress_stage_1plus(progress, 2)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 2')

    print("stage: 3, 生成目标集群tserver目录")
    # stage: 3, 生成目标集群tserver目录
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
            cmd = 'sudo sp_kudu fs format -fs_data_dirs=%s -fs_wal_dir=%s' % (','.join(new_tserver_fs_data_dirs), new_tserver_fs_wal_dir)
            run_ssh_cmd_with_password(cmd, new_tserver_host, args.ssh_port, 'sa_cluster', pwd[new_tserver_host])
            chown_dirs(new_tserver_host, args.ssh_port,
                       new_tserver_fs_wal_dir, new_tserver_fs_data_dirs, 'kudu:sa_group', pwd)
        progress = set_progress_stage_1plus(progress, 3)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 3')

    # stage: 4, 生成目标集群tserver数据
    print("stage: 4, 生成目标集群tserver数据")
    if progress['stage'] < 4:
        # 分配拷贝任务
        print("4.1----分配拷贝任务")
        os.chdir(root_path)
        if 'clone_stage' not in progress:
            i = 0
            j = 0
            new_tserver_port = new_1st_tserver.split(':')[1]
            old_tserver_list = get_cluster_tserver_list(args.local_source_cluster)
            # 生成clone task
            for old_tserver in old_tserver_list:
                j = j % len(new_tserver_hosts)
                task_name = 'task_%s' % i
                progress[task_name] = {}
                progress[task_name]['kudu_tool_path'] = args.kudu_tool_path
                progress[task_name]['ssh_port'] = args.ssh_port
                progress[task_name]['from_tserver'] = old_tserver
                progress[task_name]['to_tserver'] = new_tserver_hosts[j]
                progress[task_name]['from_master'] = args.local_source_cluster
                progress[task_name]['to_tserver_fs_data_dirs'] = new_tserver_fs_data_dirs
                progress[task_name]['to_tserver_fs_wal_dir'] = new_tserver_fs_wal_dir
                progress[task_name]['parallel'] = args.parallel
                progress[task_name]['to_tserver_port'] = new_tserver_port
                progress[task_name]['clone_stage'] = 0

                i += 1
                j += 1
                with open(progress_file, 'w+') as f:
                    yaml.dump(progress, f)

        # 执行拷贝命令
        print('4.2----多进程执行拷贝命令')
        threads = []
        # 如果thread比task还多，那么thread = task
        task_num = len(old_tserver_list)
        thread_num = min(args.thread_num, task_num)
        task_per_thread = int(task_num / thread_num)
        begin_index = 0
        end_index = 0
        for i in range(thread_num):
            end_index = begin_index + task_per_thread - 1
            end_index = min(end_index, task_num - 1)
            t = multiprocessing.Process(target=clone_data, args=(progress_file, progress, i, begin_index, end_index))
            threads.append(t)
            t.start()
            begin_index = end_index + 1
        for i in range(len(threads)):
            t = threads[i]
            t.join()
        # 重新加载progress file
        with open(progress_file, 'r') as f:
            progress = yaml.load(f, Loader=yaml.FullLoader)
            if not progress:
                die('progress_file.yaml is error.')
        # 检查每个进程的task stage，只有task stage达到1才算成功
        task_fail_flag = False
        for i in range(thread_num):
            thread_name = "thread_%s" % i
            if int(progress[thread_name]['task_stage']) != 1:
                task_fail_flag = True
                print("进程:%s 执行失败，请检查结果并重新执行" % i)
        if task_fail_flag is True:
            sys.exit(1)
        # 所有进程跑完后，更新进度
        progress = set_progress_stage_1plus(progress, 4)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 4')
    print('已完成,请手动启动目标集群')


def clone_data(progress_file, progress, pid, begin_index, end_index):
    task_id = begin_index
    thread_name = "thread_%s" % pid
    _lock.acquire()
    # 先读出来，再修改，再写进去
    with open(progress_file, 'r') as f:
        progress = yaml.load(f, Loader=yaml.FullLoader)
        if not progress:
            die('progress_file.yaml is error.')
    progress[thread_name] = {}
    progress[thread_name]['task_stage'] = 0
    with open(progress_file, 'w+') as f:
        yaml.dump(progress, f)
    _lock.release()
    if progress[thread_name]['task_stage'] < 1:
        while task_id <= end_index:
            task_name = 'task_%s' % task_id
            kudu_tool_path = progress[task_name]['kudu_tool_path']
            ssh_port = progress[task_name]['ssh_port']
            from_tserver = progress[task_name]['from_tserver']
            to_tserver = progress[task_name]['to_tserver']
            from_master = progress[task_name]['from_master']
            to_tserver_fs_data_dirs = progress[task_name]['to_tserver_fs_data_dirs']
            to_tserver_fs_wal_dir = progress[task_name]['to_tserver_fs_wal_dir']
            parallel = progress[task_name]['parallel']
            to_tserver_port = progress[task_name]['to_tserver_port']
            pwd = progress['password']
            user_sa = "sa_cluster"
            tmp_kudu_tool_path = "/home/sa_cluster/kudu"
            # 1. 拷贝工具
            print("task:%s clone kudu tool" % task_name)
            if progress[task_name]['clone_stage'] < 1:
                # 拷贝前，先检查目标集群的kudu_tool是否已经存在，存在则删除
                cmd = 'if [ -f %s ]; then sudo mv %s %s.bak; fi' % (tmp_kudu_tool_path, tmp_kudu_tool_path, tmp_kudu_tool_path)
                run_ssh_cmd_with_password(cmd, to_tserver, ssh_port, user_sa, pwd[to_tserver])
                copy_file_to_remote_with_password(to_tserver, ssh_port, user_sa, pwd[to_tserver], kudu_tool_path, tmp_kudu_tool_path)
                _lock.acquire()
                # 先读出来，再修改，再写进去
                with open(progress_file, 'r') as f:
                    progress = yaml.load(f, Loader=yaml.FullLoader)
                    if not progress:
                        die('progress_file.yaml is error.')
                set_progress_stage_clone_stage(progress, 1, task_name)
                with open(progress_file, 'w+') as f:
                    yaml.dump(progress, f)
                _lock.release()
            else:
                print("task:%s skip clone stage 1")
            # 2. 修改工具权限
            print("task:%s chown kudu tool" % task_name)
            if progress[task_name]['clone_stage'] < 2:
                run_ssh_cmd_with_password('sudo chown kudu:sa_group %s' % tmp_kudu_tool_path, to_tserver, ssh_port, user_sa, pwd[to_tserver])
                _lock.acquire()
                # 先读出来，再修改，再写进去
                with open(progress_file, 'r') as f:
                    progress = yaml.load(f, Loader=yaml.FullLoader)
                    if not progress:
                        die('progress_file.yaml is error.')
                set_progress_stage_clone_stage(progress, 2, task_name)
                with open(progress_file, 'w+') as f:
                    yaml.dump(progress, f)
                _lock.release()
            else:
                print("task:%s skip clone stage 2")
            # 3. clone tserver 数据
            print("task:%s clone tserver data" % task_name)
            if progress[task_name]['clone_stage'] < 3:
                cmd = 'sudo sh -c "%s local_replica clone %s %s -fs_data_dirs=%s -fs_wal_dir=%s -num_max_threads=%s -target_port=%s -rewrite_config=true"' % (tmp_kudu_tool_path, from_tserver, from_master, ','.join(to_tserver_fs_data_dirs), to_tserver_fs_wal_dir, parallel, to_tserver_port)
                run_ssh_cmd_with_password(cmd, to_tserver, ssh_port, user_sa, pwd[to_tserver])
                _lock.acquire()
                # 先读出来，再修改，再写进去
                with open(progress_file, 'r') as f:
                    progress = yaml.load(f, Loader=yaml.FullLoader)
                    if not progress:
                        die('progress_file.yaml is error.')
                set_progress_stage_clone_stage(progress, 3, task_name)
                with open(progress_file, 'w+') as f:
                    yaml.dump(progress, f)
                _lock.release()
            else:
                print("task:%s skip clone stage 3")
            # 4. 修改wal_dir权限
            print("task:%s chown wal dir" % task_name)
            if progress[task_name]['clone_stage'] < 4:
                cmd = 'sudo chown kudu:sa_group `dirname %s`' % to_tserver_fs_wal_dir
                run_ssh_cmd_with_password(cmd, to_tserver, ssh_port, user_sa, pwd[to_tserver])
                _lock.acquire()
                # 先读出来，再修改，再写进去
                with open(progress_file, 'r') as f:
                    progress = yaml.load(f, Loader=yaml.FullLoader)
                    if not progress:
                        die('progress_file.yaml is error.')
                set_progress_stage_clone_stage(progress, 4, task_name)
                with open(progress_file, 'w+') as f:
                    yaml.dump(progress, f)
                _lock.release()
            else:
                print("task:%s skip clone stage 4")
            # 5. 修改data_dir权限
            print("task:%s chown data dir" % task_name)
            if progress[task_name]['clone_stage'] < 5:
                cmd = 'sudo chown -R kudu:sa_group `dirname %s`' % ' '.join(to_tserver_fs_data_dirs)
                run_ssh_cmd_with_password(cmd, to_tserver, ssh_port, user_sa, pwd[to_tserver])
                _lock.acquire()
                # 先读出来，再修改，再写进去
                with open(progress_file, 'r') as f:
                    progress = yaml.load(f, Loader=yaml.FullLoader)
                    if not progress:
                        die('progress_file.yaml is error.')
                set_progress_stage_clone_stage(progress, 5, task_name)
                with open(progress_file, 'w+') as f:
                    yaml.dump(progress, f)
                _lock.release()
            else:
                print("task:%s skip clone stage 5")
            task_id = task_id + 1
        # 所有task执行完成了，更新task stage
        _lock.acquire()
        # 先读出来，再修改，再写进去
        with open(progress_file, 'r') as f:
            progress = yaml.load(f, Loader=yaml.FullLoader)
            if not progress:
                die('progress_file.yaml is error.')
        progress[thread_name]['task_stage'] = 1
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
        _lock.release()
    else:
        print("skip stage 1")


if __name__ == '__main__':
    print("stage 0: 开始执行脚本，进行前期的检查")
    main()
