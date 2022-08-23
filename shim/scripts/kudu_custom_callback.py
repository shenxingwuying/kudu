"""
Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
@author dengke(dengke@sensorsdata.cn)
@brief
"""

import os
import re
import subprocess
import time

from hyperion_utils import shell_utils


# ksyncer uuid
KSYNCER_UUID = 'beefbeefbeefbeefbeefbeefbeefbeef'
# 缩容操作的当前状态
DECOMMISSION_SUCCESS = 0      # 已经成功完成
DECOMMISSION_IN_PROGRESS = 1  # 进行中
DECOMMISSION_NOT_STARTED = 2  # 未开始


def master_addresses(context):
    params = context.get_params()
    master_hosts = params['cluster_node_info']['kudu_master']['nodes']
    master_port = params['cluster_port_info']['kudu_master_ports']['service_port']['port']
    return ','.join(['%s:%s' % (h, master_port) for h in master_hosts])


def execute_cluster_ksck(context):
    logger = context.get_logger()  # 获取 logger 实例
    ksck_cmd = "kudu cluster ksck '%s'" % master_addresses(context)
    ksck_output = shell_utils.run_cmd(ksck_cmd, logger.debug)
    return ksck_output['ret'], ksck_output['stdout']


def check_kudu_ok(context):
    logger = context.get_logger()  # 获取 logger 实例
    ksck_cmd = "kudu cluster ksck '%s'" % master_addresses(context)
    shell_utils.check_call(ksck_cmd, logger.debug)


def rebalance_data(context):
    params = context.get_params()  # 获取 param.json 的内容
    runtime_dir = params['runtime_params']['runtime_dir']
    ouput_file_dir = os.path.join(runtime_dir, 'kudu_rebalance')
    rebalance_cmd = 'kudu cluster rebalance %s &>> %s' % (
        master_addresses(context), ouput_file_dir
    )
    subprocess.Popen(rebalance_cmd, start_new_session=True, shell=True)


def check_balance(context):
    logger = context.get_logger()  # 获取 logger 实例
    check_cmd = 'kudu cluster rebalance %s -report_only' % master_addresses(context)
    output = shell_utils.check_output(check_cmd, logger.debug)
    # compile 函数用于编译正则表达式，生成一个 Pattern 对象
    pat = re.compile(r"Minimum Replica Count.*?\n\n", re.S)
    lines = pat.findall(output)[0].split('\n')[:-2]
    min_count = int(lines[0].split()[4])
    max_count = int(lines[1].split()[4])
    average_cout = float(lines[2].split()[4])
    if (max_count - average_cout) <= 1 and (average_cout - min_count) <= 1:
        logger.info('Kudu rebalance done.')
    else:
        # 目前框架不支持返回值判断，没有均衡完成直接抛异常
        raise Exception('Kudu rebalance not completed!!!')


def tserver_count(context):
    params = context.get_params()  # 获取 param.json 的内容
    tserver_hosts = params['cluster_node_info']['kudu_tserver']['nodes']
    return (len(tserver_hosts))


def check_kudu_table_rf(context, ksck_output):
    # compile 函数用于编译正则表达式，生成一个 Pattern 对象
    pat = re.compile(r"Summary by table.*?\n\n", re.S)
    find_out = pat.findall(ksck_output)
    if len(find_out) != 0:
        for line in find_out[0].split('\n')[3:-2]:
            if line.split()[2] == '1':
                raise Exception('Table %s RF == 1, please deal with it' % line.split()[0])


def get_tserver_uuid_map(context):
    get_uuid_cmd = "kudu tserver list %s -columns=rpc-addresses,uuid -format=space" % (master_addresses(context))
    tserver_uuid_map = dict()
    logger = context.get_logger()  # 获取 logger 实例
    for line in shell_utils.check_output(get_uuid_cmd, logger.debug).split('\n')[:-1]:
        uuid = line.split()[1]
        if uuid != KSYNCER_UUID:
            tserver_uuid_map[line.split()[0]] = uuid
    return tserver_uuid_map


def check_server_alive(context, role, address):
    logger = context.get_logger()  # 获取 logger 实例
    check_cmd = ''
    if role == 'kudu_tserver':
        check_cmd = 'kudu tserver status %s' % address
    elif role == 'kudu_master':
        check_cmd = 'kudu master status %s' % address
    return not shell_utils.run_cmd(check_cmd, logger.debug)['ret']


def check_host_decomission(context):
    """
    返回码说明
    DECOMMISSION_SUCCESS: 已经成功完成
    DECOMMISSION_IN_PROGRESS: 进行中
    DECOMMISSION_NOT_STARTED: 未开始
    """
    hosts = context.get_scale_hosts()  # 获取扩缩容的机器列表
    logger = context.get_logger()  # 获取 logger 实例
    uuid_list = []
    host_uuid_map = dict()
    tserver_uuid_map = get_tserver_uuid_map(context)
    for tserver in tserver_uuid_map.keys():
        host_uuid_map[tserver.split(':')[0]] = tserver_uuid_map[tserver]
    for h in hosts:
        uuid_list.append(host_uuid_map[h])
    check_decomission_cmd = \
        "kudu cluster rebalance %s -ignored_tservers=%s -move_replicas_from_ignored_tservers=true -report_only" % (
            master_addresses(context), ','.join(uuid_list))
    logger.info('execute %s' % check_decomission_cmd)
    check_output = shell_utils.check_output(check_decomission_cmd, logger.debug)
    pat = re.compile(r"Server UUID.*?\n\n", re.S)
    is_done = True
    for line in pat.findall(check_output)[0].split('\n')[2:-2]:
        if line.split()[2] != '0' and KSYNCER_UUID != line.split()[0]:
            is_done = False
    if is_done:
        return DECOMMISSION_SUCCESS
    _, ksck_out = execute_cluster_ksck(context)
    pat = re.compile(r"Tablet Server States.*?\n\n", re.S)
    if len(pat.findall(ksck_out)) == 0:
        return DECOMMISSION_NOT_STARTED
    maintenance_uuids = set()
    for line in pat.findall(ksck_out)[0].split('\n')[3:-2]:
        maintenance_uuids.add(line.split()[0])
    if len(set(uuid_list).difference(maintenance_uuids)) == 0:
        params = context.get_params()  # 获取 param.json 的内容
        runtime_dir = params['runtime_params']['runtime_dir']
        ouput_file_dir = os.path.join(runtime_dir, 'kudu_decommission')
        logger.info('decommission output file: %s' % ouput_file_dir)
        return DECOMMISSION_IN_PROGRESS
    else:
        return DECOMMISSION_NOT_STARTED


def change_server_unavailable_sec(context, roles, value):
    logger = context.get_logger()  # 获取 logger 实例
    if 'kudu_tserver' in roles:
        for tserver in get_tserver_uuid_map(context).keys():
            if check_server_alive(context, 'kudu_tserver', tserver):
                cmd = 'kudu tserver set_flag %s follower_unavailable_considered_failed_sec %s -force' % (
                    tserver, value
                )
                shell_utils.check_call(cmd, logger.debug)
    if 'kudu_master' in roles:
        for master in master_addresses(context).split(','):
            cmd = 'kudu master set_flag %s follower_unavailable_considered_failed_sec %s -force' % (
                master, value
            )
            shell_utils.check_call(cmd, logger.debug)


def restart_masters(context):
    # 滚动重启 master
    logger = context.get_logger()  # 获取 logger 实例
    change_server_unavailable_sec(context, {'kudu_master', 'kudu_tserver'}, 7200)
    for master_address in master_addresses(context).split(','):
        logger.info('restart kudu master %s' % master_address)
        cmd = 'mothershipadmin restart -m kudu -r kudu_master --host %s' % master_address.split(':')[0]
        shell_utils.check_call(cmd, logger.debug)
    change_server_unavailable_sec(context, {'kudu_tserver'}, 300)


def pre_scale_up_check(context):
    """扩容角色前置检查
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    params = context.get_params()  # 获取 param.json 的内容
    logger = context.get_logger()  # 获取 logger 实例
    scale_role = context.get_scale_role()  # 获取扩缩容的角色
    exec_role = context.get_scale_exec_role()  # 获取当前执行 shim 的角色
    hosts = context.get_scale_hosts()  # 获取扩缩容的机器列表
    info = f'{scale_role} {exec_role} {hosts} {list(params.keys())}'
    logger.info(info)
    # kudu_master 不能进行扩容操作
    if 'kudu_master' in scale_role:
        raise Exception('Do not allow add kudu-master role!!!')
    # kudu_collector 不能进行扩容操作
    if 'kudu_collector' in scale_role:
        raise Exception('Do not allow add kudu-collector role!!!')
    ret, _ = execute_cluster_ksck(context)
    if ret == 1:
        raise Exception('Kudu cluster is unhealthy, can not do add tserver role!!!')
    # 增加节点前之前需要确保新节点的 wal_dir、data_dirs 目录为空，这个步骤在安装时进行检查
    return f'{logger} {info}'


def pre_scale_up_start(context):
    """扩容角色启动前回调
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    logger = context.get_logger()
    logger.info('Kudu pre scale up start')  # 日志在执行机器上的 /sensorsdata/main/logs/mothership/shim_callback.log


def post_scale_up_start(context):
    """扩容启动后回调
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    logger = context.get_logger()
    ret, _ = execute_cluster_ksck(context)
    if ret == 1:
        # 日志在执行机器上的 /sensorsdata/main/logs/mothership/shim_callback.log
        logger.warn('after add tserver, kudu ksck not ok')
    rebalance_data(context)


def post_scale_up_check(context):
    """扩容启动后检查回调
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    return check_balance(context)


def pre_scale_down_check(context):
    """缩容前置检查
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    params = context.get_params()  # 获取 param.json 的内容
    logger = context.get_logger()  # 获取 logger 实例
    scale_role = context.get_scale_role()  # 获取扩缩容的角色
    exec_role = context.get_scale_exec_role()  # 获取当前执行 shim 的角色
    hosts = context.get_scale_hosts()  # 获取扩缩容的机器列表
    info = f'{scale_role} {exec_role} {hosts} {list(params.keys())}'
    logger.info(info)
    # 下节点前有以下限制：
    # 1.集群健康
    # 2.不允许下tserver节点
    # 3.下tserver节点前、节点数需要大于3
    # 4.检查没有单副本表
    # 5.每次下节点只操作一个节点（不能完全依赖上层框架，自己也需要检查）
    if 'kudu_master' in scale_role:
        raise Exception('Do not allow remove kudu-master role!!!')
    if 'kudu_collector' in scale_role:
        raise Exception('Do not allow remove kudu-collector role!!!')
    if 'kudu_tserver' in scale_role:
        ret, ksck_output = execute_cluster_ksck(context)
        if ret == 1:
            raise Exception('kudu ksck not ok!')
        if tserver_count(context) - len(hosts) < 3:
            raise Exception('after handle, kudu alive tservers num will littler than 3!')
        check_kudu_table_rf(context, ksck_output)
        status = check_host_decomission(context)
        if status == DECOMMISSION_IN_PROGRESS:
            raise Exception('please decommission tserver %s first! current status %s' % (hosts, status))
    return f'{logger} {info}'


def decommission_callback(context):
    """缩容任务回调，用于触发缩容任务
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    # 把计划下线的 tserver 中的数据迁移到其它 tservers 中
    # 仅支持正常的 tservers 数量多于三台的集群（不执行 decommission 的 tserver 必须大于三台）
    # 执行该方法后执行，通过 decommission_complete_inspection() 检查缩容是否完成
    scale_role = context.get_scale_role()  # 获取扩缩容的角色
    logger = context.get_logger()  # 获取 logger 实例
    hosts = context.get_scale_hosts()  # 获取扩缩容的机器列表
    if 'kudu_tserver' not in scale_role:
        raise Exception('Kudu can not decommision for %s' % (scale_role))
    ret, ksck_output = execute_cluster_ksck(context)
    if ret != 0:
        raise Exception('kudu ksck not ok!')
    if tserver_count(context) - len(hosts) < 3:
        raise Exception('after decommission, kudu alive tservers num will littler than 3!')
    check_kudu_table_rf(context, ksck_output)
    host_uuid_map = dict()
    tserver_uuid_map = get_tserver_uuid_map(context)
    for tserver in tserver_uuid_map.keys():
        host_uuid_map[tserver.split(':')[0]] = tserver_uuid_map[tserver]
    uuid_list = []
    # 把需要 decommision 的 tserver 切入 maintenance 状态
    for h in hosts:
        enter_maintenance_cmd = \
            "kudu tserver state enter_maintenance %s %s -allow_missing_tserver=true" % (
                master_addresses(context), host_uuid_map[h])
        shell_utils.check_call(enter_maintenance_cmd, logger.debug)
        uuid_list.append(host_uuid_map[h])
    params = context.get_params()  # 获取 param.json 的内容
    runtime_dir = params['runtime_params']['runtime_dir']
    ouput_file_dir = os.path.join(runtime_dir, 'kudu_decommission')
    # 数据迁移
    decommission_cmd = \
        "kudu cluster rebalance %s -ignored_tservers=%s -move_replicas_from_ignored_tservers=true &>> %s" % (
            master_addresses(context), ','.join(uuid_list), ouput_file_dir)
    shell_utils.check_call("echo '%s' >> %s" % (decommission_cmd, ouput_file_dir), logger.debug)
    subprocess.Popen(decommission_cmd, start_new_session=True, shell=True)


def wait_tablet_start_recover(context, polling_cnt=20):
    _, ksck_out = execute_cluster_ksck(context)
    cnt = 0
    change_server_unavailable_sec(context, {'kudu_master', 'kudu_tserver'}, 5)
    while 'replica(s) not RUNNING' in ksck_out and cnt < polling_cnt:
        time.sleep(5)
        _, ksck_out = execute_cluster_ksck(context)
    change_server_unavailable_sec(context, {'kudu_master', 'kudu_tserver'}, 300)
    if 'replica(s) not RUNNING' in ksck_out:
        raise Exception('kudu tablet cannot recover succeed in 100s, please execute ksck check it!!!')


def decommission_complete_inspection(context):
    """检查缩容是否完成，由云平台轮询回调，缩容没有完成直接抛异常
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    logger = context.get_logger()  # 获取 logger 实例
    ret, _ = execute_cluster_ksck(context)
    if ret == 1:
        logger.warn('after restart kudu master, kudu ksck not ok!')
        wait_tablet_start_recover(context)


def post_scale_down_stop(context):
    """缩容停角色后置任务
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    # 删除节点后 滚动重启 master
    scale_role = context.get_scale_role()  # 获取扩缩容的角色
    if 'kudu_tserver' in scale_role:
        restart_masters(context)


def post_scale_down_check(context):
    """缩容停角色后置检查
    扩缩容接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272735945
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515
    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    logger = context.get_logger()
    logger.info('post_scale_down_check done')  # 日志在执行机器上的 /sensorsdata/main/logs/mothership/shim_callback.log
