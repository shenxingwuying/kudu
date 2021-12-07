#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import commands
import getpass
import json
import os
import socket
import sys
import time
import uuid
import yaml

from argparse import ArgumentParser

master_meta_uuid = '0' * 32
ksyncer_uuid = 'beef' * 8


def run_cmd(cmd):
    print('going to run: "' + cmd + '"')
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print('execute %s error' % cmd)
        print('status: ' + str(status))
        print('output: ')
        print(output)
        sys.exit(1)
    return output


def check_cluster(cluster_masters_str):
    masters_list = cluster_masters_str.split(',')
    if len(masters_list) != 1 and len(masters_list) != 3:
        return False
    output = run_cmd('kudu cluster ksck %s' % cluster_masters_str)
    return output.endswith('OK')


def check_ssh(hosts, ssh_port):
    for host in hosts:
        if not check_service_connected(host, ssh_port):
            return False
    return True


def get_cluster_leader_master(cluster_masters_str):
    output = run_cmd('kudu master list %s -columns=rpc-addresses,role -format=json' % cluster_masters_str)
    masters = json.loads(output)
    for master in masters:
        if master['role'] == 'LEADER':
            return master['rpc-addresses']
    return None


def get_cluster_tserver_list(cluster_masters_str):
    tserver_list = []
    output = run_cmd('kudu tserver list %s -columns=rpc-addresses,uuid -format=json' % cluster_masters_str)
    tservers = json.loads(output)
    for tserver in tservers:
        if not tserver['uuid'] == ksyncer_uuid:
            tserver_list.append(tserver['rpc-addresses'])

    if len(tserver_list) == 0:
        print('Cluster(%s) has 0 tservers' % cluster_masters_str)
        sys.exit(1)

    return tserver_list


def get_server_config(role, rpc_addr, config):
    assert (role == 'master' or role == 'tserver')
    output = run_cmd('kudu %s get_flags %s -flags=%s -format=json' % (role, rpc_addr, config))
    configs = json.loads(output)
    assert (len(configs) == 1)
    assert ('value' in configs[0])
    return configs[0]['value']


def backup_dirs(host, ssh_port, wal_dir, data_dirs, timestamp):
    run_cmd('ssh sa_cluster@%s -p %d "if [ -d %s ]; then sudo mv %s %s.bak.%s; fi"' %
            (host, ssh_port, wal_dir, wal_dir, wal_dir, timestamp))
    for data_dir in data_dirs:
        run_cmd('ssh sa_cluster@%s -p %d "if [ -d %s ]; then sudo mv %s %s.bak.%s; fi"' %
                (host, ssh_port, data_dir, data_dir, data_dir, timestamp))


def chown_dirs(host, ssh_port, wal_dir, data_dirs, owner):
    run_cmd('ssh sa_cluster@%s -p %d sudo chown -R %s `dirname %s`' % (host, ssh_port, owner, wal_dir))
    for data_dir in data_dirs:
        run_cmd('ssh sa_cluster@%s -p %d sudo chown -R %s `dirname %s`' % (host, ssh_port, owner, data_dir))


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
    run_cmd('sp_kudu fs format '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s '
            '-uuid=%s'
            % (fs_wal_dir, fs_data_dirs, new_master_uuids[index]))
    run_cmd('sp_kudu local_replica copy_from_remote %s %s '
            '-fs_wal_dir=%s '
            '-fs_data_dirs=%s'
            % (master_meta_uuid, old_master_leader, fs_wal_dir, fs_data_dirs))
    run_cmd('sp_kudu local_replica cmeta rewrite_raft_config %s %s '
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
    parser.add_argument('-k', '--kudu_tool_path', default='/usr/bin/kudu',
                        help='Path of the Kudu CLI tool, should support leader replica only clone')
    parser.add_argument('--ignore_dest_cluster_ksck', type=bool, default=False,
                        help='Not to check dest cluster health status')
    parser.add_argument('--continue_mode', type=bool, default=False,
                        help="""Whether to continue previous work. If --continue_mode is True,
                        --ignore_dest_cluster_ksck must be True too.""")
    args = parser.parse_args()

    # Post processing checks
    if not check_cluster(args.local_source_cluster):
        parser.print_usage()
        die('Source cluster must be reachable and in heath status')
    if not args.ignore_dest_cluster_ksck and not check_cluster(args.dest_cluster):
        parser.print_usage()
        die('Destination cluster must be reachable and in heath status')
    if args.parallel < 1 or args.parallel > 16:
        parser.print_usage()
        die('Parallel must be in range [1, 16]')
    if not os.path.isfile(args.kudu_tool_path):
        parser.print_usage()
        die('Kudu CLI tool is not exist')
    if args.continue_mode and not args.ignore_dest_cluster_ksck:
        parser.print_usage()
        die('If --continue_mode is True, --ignore_dest_cluster_ksck must be True too')

    return args


def set_progress_stage_1(new_master_fs_wal_dir, new_master_fs_data_dirs_list,
                         new_tserver_list, new_tserver_fs_wal_dir, new_tserver_fs_data_dirs):
    progress = {'stage': 1, 'dest_cluster': {}}
    progress['dest_cluster']['master'] = {}
    progress['dest_cluster']['master']['fs_wal_dir'] = new_master_fs_wal_dir
    progress['dest_cluster']['master']['fs_data_dirs'] = new_master_fs_data_dirs_list
    progress['dest_cluster']['tserver'] = {}
    progress['dest_cluster']['tserver']['nodes'] = new_tserver_list
    progress['dest_cluster']['tserver']['fs_wal_dir'] = new_tserver_fs_wal_dir
    progress['dest_cluster']['tserver']['fs_data_dirs'] = new_tserver_fs_data_dirs
    return progress


def set_progress_stage_1plus(progress, stage):
    progress['stage'] = stage
    return progress


def set_progress_stage_clone_cmds(progress, clone_cmds):
    progress['clone_cmds'] = clone_cmds
    return progress


def set_progress_stage_clone_stage(progress, clone_stage):
    progress['clone_stage'] = clone_stage
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
            progress = yaml.load(f)
            if not progress:
                die('progress_file.yaml is error.')

    print('Current stage is %d' % progress['stage'])
    # stage: 1, 生成目标集群配置
    # 目标集群masters
    new_masters_list = args.dest_cluster.split(',')
    new_master_hosts = map(lambda master: master.split(':')[0], new_masters_list)
    if not progress or 'stage' not in progress or progress['stage'] < 1:
        print('Start stage 1')
        # 请确保其父目录是 kudu:sa_group 权限！！！
        new_master_fs_wal_dir = get_server_config('master', new_masters_list[0], 'fs_wal_dir')

        # 请确保其父目录是 kudu:sa_group 权限！！！
        new_master_fs_data_dirs_list = get_server_config('master', new_masters_list[0], 'fs_data_dirs').split(',')
        if len(new_master_fs_data_dirs_list) != 1:
            die('Dest master should have only 1 dir for config fs_data_dirs')

        # 目标集群tservers
        new_tserver_list = get_cluster_tserver_list(args.dest_cluster)
        new_tserver_hosts = map(lambda tserver: tserver.split(':')[0], new_tserver_list)
        new_1st_tserver = new_tserver_list[0]

        # 检查ssh连通性
        if not check_ssh(new_master_hosts + new_tserver_hosts, args.ssh_port):
            die('Some host could not ssh')

        # 请确保其父目录是 kudu:sa_group 权限！！！
        new_tserver_fs_wal_dir = get_server_config('tserver', new_1st_tserver, 'fs_wal_dir')

        # 请确保其父目录是 kudu:sa_group 权限！！！
        new_tserver_fs_data_dirs = get_server_config('tserver', new_1st_tserver, 'fs_data_dirs').split(',')

        # 更新进度
        progress = set_progress_stage_1(new_master_fs_wal_dir, new_master_fs_data_dirs_list,
                                        new_tserver_list, new_tserver_fs_wal_dir, new_tserver_fs_data_dirs)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 1')

    # stage: 2, 生成目标集群master数据
    timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime())
    new_master_fs_wal_dir = progress['dest_cluster']['master']['fs_wal_dir']
    new_master_fs_data_dirs_list = progress['dest_cluster']['master']['fs_data_dirs']
    # 目标集群tservers
    new_tserver_list = progress['dest_cluster']['tserver']['nodes']
    new_tserver_hosts = map(lambda tserver: tserver.split(':')[0], new_tserver_list)
    new_1st_tserver = new_tserver_list[0]
    if progress['stage'] < 2:
        print('Start stage 2')
        # 等待目标集群停止
        while True:
            if check_all_service_down(new_masters_list + new_tserver_list):
                break
            print('若要继续做集群迁移，请停止目标集群(%s)' % args.dest_cluster)
            time.sleep(10)

        # 备份目标集群master
        for new_master_host in new_master_hosts:
            backup_dirs(new_master_host, args.ssh_port, new_master_fs_wal_dir, new_master_fs_data_dirs_list, timestamp)

        # 为新集群生成新的master uuid
        new_master_uuids = []
        new_master_peers = ''
        for new_master in new_masters_list:
            new_master_uuid = uuid.uuid4().hex
            new_master_uuids.append(new_master_uuid)
            new_master_peers += ('%s:%s ' % (new_master_uuid, new_master))

        # 生成master数据，并拷贝到目标集群
        i = 0
        old_master_leader = get_cluster_leader_master(args.local_source_cluster)
        for new_master in new_masters_list:
            os.chdir(root_path)
            new_masters_host = new_master.split(':')[0]

            # 创建临时目录
            cwd = generate_data_dir(new_masters_host, timestamp)
            fs_wal_dir = '%s/master_wal' % cwd
            fs_data_dirs = '%s/master_data' % cwd

            # 生成master数据
            generate_master_data(old_master_leader,
                                 i, new_master_uuids, new_master_peers,
                                 fs_wal_dir, fs_data_dirs)

            # 拷贝master数据
            chown_dirs(new_masters_host, args.ssh_port,
                       new_master_fs_wal_dir, new_master_fs_data_dirs_list, 'sa_cluster:sa_group')
            run_cmd('rsync -av %s/ %s:%s' % (fs_wal_dir, new_masters_host, new_master_fs_wal_dir))
            run_cmd('rsync -av %s/ %s:%s' % (fs_data_dirs, new_masters_host, new_master_fs_data_dirs_list[0]))
            chown_dirs(new_masters_host, args.ssh_port,
                       new_master_fs_wal_dir, new_master_fs_data_dirs_list, 'kudu:sa_group')

            i += 1
        progress = set_progress_stage_1plus(progress, 2)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 2')

    # stage: 3, 生成目标集群tserver目录
    new_tserver_fs_wal_dir = progress['dest_cluster']['tserver']['fs_wal_dir']
    new_tserver_fs_data_dirs = progress['dest_cluster']['tserver']['fs_data_dirs']
    if progress['stage'] < 3:
        print('Start stage 3')
        # 备份目标集群tserver
        for new_tserver_host in new_tserver_hosts:
            backup_dirs(new_tserver_host, args.ssh_port,
                        new_tserver_fs_wal_dir, new_tserver_fs_data_dirs, timestamp)

        # 创建目标集群的tserver目录
        for new_tserver_host in new_tserver_hosts:
            run_cmd('ssh sa_cluster@%s -p %d \'sudo sh -c "sp_kudu fs format '
                    '-fs_data_dirs=\'%s\' '
                    '-fs_wal_dir=\'%s\'"\''
                    % (new_tserver_host,
                       args.ssh_port,
                       ','.join(new_tserver_fs_data_dirs),
                       new_tserver_fs_wal_dir))
            chown_dirs(new_tserver_host, args.ssh_port,
                       new_tserver_fs_wal_dir, new_tserver_fs_data_dirs, 'kudu:sa_group')
        progress = set_progress_stage_1plus(progress, 3)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 3')

    # stage: 4, 生成目标集群tserver数据
    if progress['stage'] < 4:
        print('Start stage 4')
        # 生成拷贝命令
        os.chdir(root_path)
        if 'clone_stage' not in progress:
            i = 0
            j = 0
            new_tserver_port = new_1st_tserver.split(':')[1]
            old_tserver_list = get_cluster_tserver_list(args.local_source_cluster)
            clone_cmds = []
            for old_tserver in old_tserver_list:
                j = j % len(new_tserver_hosts)
                # 1. 拷贝工具
                # 2. 修改工具权限
                # 3. clone tserver
                clone_cmds.append(['rsync -av %s %s:/tmp' % (args.kudu_tool_path, new_tserver_hosts[j]),
                                   'ssh sa_cluster@%s -p %d sudo chown %s /tmp/kudu'
                                   % (new_tserver_hosts[j], args.ssh_port, 'kudu:sa_group'),
                                   'ssh sa_cluster@%s -p %d \'sudo sh -c "/tmp/kudu local_replica clone %s %s '
                                   '-fs_data_dirs=%s -fs_wal_dir=%s -num_max_threads=%d -target_port=%s '
                                   '-rewrite_config=true"\''
                                   % (new_tserver_hosts[j],
                                      args.ssh_port,
                                      old_tserver,
                                      args.local_source_cluster,
                                      ','.join(new_tserver_fs_data_dirs),
                                      new_tserver_fs_wal_dir,
                                      args.parallel,
                                      new_tserver_port),
                                   'ssh sa_cluster@%s -p %d sudo chown -R %s `dirname %s`'
                                   % (new_tserver_hosts[j], args.ssh_port, 'kudu:sa_group', new_tserver_fs_wal_dir)])
                for data_dir in new_tserver_fs_data_dirs:
                    clone_cmds.append(['ssh sa_cluster@%s -p %d sudo chown -R %s `dirname %s`'
                                       % (new_tserver_hosts[j], args.ssh_port, 'kudu:sa_group', data_dir)])
                i += 1
                j += 1
            progress = set_progress_stage_clone_stage(progress, 0)
            progress = set_progress_stage_clone_cmds(progress, clone_cmds)
            with open(progress_file, 'w+') as f:
                yaml.dump(progress, f)

        # 执行拷贝命令
        print('We plan to run cmds:')
        for i in range(len(progress['clone_cmds'])):
            for cmd in progress['clone_cmds'][i]:
                print('[%d/%d] %s' % (i, len(progress['clone_cmds']), cmd))
        clone_stage = progress['clone_stage']
        print('We have run [%d/%d]' % (clone_stage, len(progress['clone_cmds'])))
        while clone_stage < len(progress['clone_cmds']):
            print('[%s] Start clone progress [%d/%d]' %
                  (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), clone_stage, len(progress['clone_cmds'])))
            for cmd in progress['clone_cmds'][clone_stage]:
                run_cmd(cmd)

            clone_stage += 1
            progress = set_progress_stage_clone_stage(progress, clone_stage)
            with open(progress_file, 'w+') as f:
                yaml.dump(progress, f)
            print('[%s] Finish clone progress [%d/%d]' %
                  (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), clone_stage, len(progress['clone_cmds'])))

        progress = set_progress_stage_1plus(progress, 4)
        with open(progress_file, 'w+') as f:
            yaml.dump(progress, f)
    else:
        print('Skip stage 4')

    print('已完成,请手动启动目标集群')


if __name__ == '__main__':
    main()
