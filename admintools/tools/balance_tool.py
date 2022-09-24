#!/bin/env python
# -*- coding: UTF-8 -*-

import copy
import json
import logging
import os
import sys
import subprocess
import time
import threading

from collections import defaultdict

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_utils import shell_utils
from hyperion_client.config_manager import ConfigManager
from hyperion_client.deploy_info import DeployInfo

SOKU_TOOL_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'soku_tool')
if SOKU_TOOL_ROOT_PATH not in sys.path:
    sys.path.append(SOKU_TOOL_ROOT_PATH)
sys.path.append(os.path.join(os.environ['SENSORS_SOKU_HOME'], 'soku_tool', 'tools'))
from base_tool import BaseTool


class BalanceTool(BaseTool):
    example_doc = '''
soku_tool banlance start # 开始balance
'''

    # 需要设置日志等级，否则有时默认WARNING级别
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    reached = False
    best_op = [float("inf"), [], []]

    def init_parser(self, parser):
        global timeout_signal
        timeout_signal = 0
        return 0

    def run_cmd(self, cmd):
        result = shell_utils.run_cmd(cmd, self.logger.info)
        if result['ret'] != 0:
            self.logger.info('execute %s error:%s' % (cmd, result))
            sys.exit(1)
        return result

    def do(self, args):
        # 判断是否为单机环境
        if DeployInfo().get_simplified_cluster():
            self.logger.info("This cluster is standalone, no need do leader rebalance. Exit!")
            sys.exit(0)
        # 获取master地址
        master_addresses = ConfigManager().get_client_conf("sp", "kudu")['master_address']
        # 获取所有的表
        cmd = 'sp_kudu table list {master_addresses}'.format(master_addresses=master_addresses)
        res = self.run_cmd(cmd)
        tables_list = res['stdout'].strip().split('\n')
        for table in tables_list:
            # 过滤掉trashed的表
            if "TRASHED" in table:
                continue
            self.solution(master_addresses, table)
        self.logger.info('Finish leader rebalance task!')
        sys.exit(0)

    def ksck(self, master_addresses, table_filter):
        ksck_cmd = 'sp_kudu cluster ksck %s -ksck_format=json_compact -sections=TABLET_SUMMARIES -tables=%s' \
            % (master_addresses, table_filter)
        # 无法使用云平台的接口，因为含有特殊字符，云平台的接口解析不了
        p = subprocess.Popen([ksck_cmd], stdout=subprocess.PIPE, shell=True)
        stdout, _ = p.communicate()
        stdout = stdout.strip()
        if p.returncode != 0:
            self.logger.info("execute command:%s failed" % ksck_cmd)
            exit(1)
        # ksck json格式输出，partitioin key包含一个特殊编码，需要解析成cp1252
        stdout = stdout.decode("cp1252")
        return stdout

    def scheduling_uno_op(self, master_addresses, op):
        tablet, _, to_ts = op
        step_down_cmd = 'sp_kudu tablet leader_step_down %s %s -new_leader_uuid=%s' % (master_addresses, tablet, to_ts)
        self.run_cmd(step_down_cmd)

    # ts_map: <tserver, all leader tablets in it>
    # tablet_tuple_list: <tablet_id, tserver of leader replica, all tservers of replicas>
    def parse_ksck(self, ksck_output):
        tablets = json.loads(ksck_output)
        ts_map = defaultdict(list)
        tablet_tuple_list = list()
        for tablet in tablets['tablet_summaries']:
            replica_tservers = []
            tablet_id = tablet['id']
            leader_tserver = tablet['master_cstate']['leader_uuid']
            ts_map[leader_tserver].append(tablet_id)
            for replica_ts in tablet['master_cstate']['voter_uuids']:
                if replica_ts not in ts_map:
                    ts_map[replica_ts] = list()
                replica_tservers.append(replica_ts)
            tablet_tuple_list.append((tablet_id, leader_tserver, replica_tservers))
        return tablet_tuple_list, ts_map

    # 计算leader tablet平均值
    # num_leader_tablets / num_tservers
    def cal_mean(self, tserver_leader_tablets_map):
        num_leader_tablets = sum([len(_) for _ in tserver_leader_tablets_map.values()])
        num_tservers = len(tserver_leader_tablets_map)
        return num_leader_tablets / num_tservers

    # 计算方差，方差代表当前拓扑结构的均衡分数
    def eval_score(self, tserver_leader_tablets_map):
        mean_leader_tablets = self.cal_mean(tserver_leader_tablets_map)
        variance = sum([((len(_)) - mean_leader_tablets) ** 2 for _ in tserver_leader_tablets_map.values()])
        return variance

    def report(self, tserver_leader_tablets_map):
        mean_cnt = self.cal_mean(tserver_leader_tablets_map)
        num_leader_tablets = sum([len(_) for _ in tserver_leader_tablets_map.values()])
        num_tservers = len(tserver_leader_tablets_map)
        self.logger.info("Total tservers: %s, Total leader tablets: %s, Mean: %s" % (num_tservers, num_leader_tablets, mean_cnt))
        for tserver, leader_tablets in tserver_leader_tablets_map.items():
            skew_value = len(leader_tablets) - mean_cnt
            if abs(skew_value) < 1:
                self.logger.info('ts %s has %d avg is %f' % (tserver, len(leader_tablets), mean_cnt))
                continue
            elif skew_value > 0:
                self.logger.info('ts %s execceds %f' % (tserver, skew_value))
            else:
                self.logger.info('ts %s behinds %f' % (tserver, skew_value))

    def dfs(self, tablet_tuple_list, ops, leader_map, index, cur_score):
        # 搜索任务超时，需要结束当前任务
        global timeout_signal
        if timeout_signal == 1:
            return
        if self.reached:
            return
        if index >= len(tablet_tuple_list):
            score = self.eval_score(leader_map)
            # 不能比当前拓扑结构的分数还高
            if score >= cur_score:
                return
            if score < self.best_op[0]:
                self.best_op[0], self.best_op[1], self.best_op[2] = score, ops[::], copy.deepcopy(leader_map)
                if score < 1:
                    self.reached = True
            return
        tablet_id, leader, ts_list = tablet_tuple_list[index]
        for ts in ts_list:
            leader_map[ts].append(tablet_id)
            if leader != ts:
                ops.append((tablet_id, leader, ts))
            self.dfs(tablet_tuple_list, ops[::], leader_map, index + 1, cur_score)
            if leader != ts:
                ops.pop()
            leader_map[ts].remove(tablet_id)

    # tablet_tuple_list: 记录每个tablet的leader 副本所在的tserver，所有副本的所在的tserver
    def solution(self, master_addresses, table_filter, only_report=False):
        ksck_output = self.ksck(master_addresses, table_filter)
        tablet_tuple_list, tserver_leader_tablets_map = self.parse_ksck(ksck_output)
        score = self.eval_score(tserver_leader_tablets_map)
        self.report(tserver_leader_tablets_map)
        # 方差小于1，则集群已经平衡
        if score < 1:
            self.logger.info('table %s was rebalanced' % table_filter)
            return 0

        leader_map = defaultdict(list)
        for tserver in tserver_leader_tablets_map.keys():
            leader_map[tserver]
        self.reached = False
        self.dfs(tablet_tuple_list, [], leader_map, 0, score)
        # 搜索任务执行完，需要取消计时器任务
        global timeout_signal
        timeout_signal = 2
        if len(self.best_op[1]) <= 0:
            self.logger.info("Can not found a solution.")
            return 0
        self.logger.info("Migrate solution is as follow:")
        for op in self.best_op[1]:
            tablet, from_tserver, to_tserver = op
            self.logger.info("Tablet:%s will migrate from %s to %s" % (tablet, from_tserver, to_tserver))
        self.logger.info("After migrating, the tablet distribution will be:")
        self.report(self.best_op[2])
        # 仅打印迁移方案，不具体执行
        if only_report:
            return 0
        for op in self.best_op[1]:
            self.scheduling_uno_op(master_addresses, op)
        try_time = 0
        has_to_try = True
        while try_time < 5 and has_to_try:
            has_to_try = not self.try_and_gen_score(master_addresses, table_filter)
            try_time += 1
        return 1 if has_to_try else 0

    def try_and_gen_score(self, master_addresses, table_filter):
        try:
            time.sleep(5)
            self.logger.info('waiting for skew info after gen')
            ksck_output = self.ksck(master_addresses, table_filter)
            _, ts_map = self.parse_ksck(ksck_output)
            score = self.eval_score(ts_map)
            self.logger.info('after rebalance skew map')
            self.report(ts_map)
            return score < 1
        except Exception:
            self.logger.info('cluster not rebalance done, waiting')
            return False

def timeout_thread():
    global timeout_signal
    i = 0
    while i < 600:
        time.sleep(1)
        if timeout_signal == 2:
            return
        i = i + 1
    timeout_signal = 1
    return

def run(master_address, table_str):
    BalanceTool().solution(master_address, table_str)
    global timeout_signal
    timeout_signal = 2
    return

def balance_table(master_addr, table):
    threads = []
    threads.append(threading.Thread(target=timeout_thread))
    threads.append(threading.Thread(target=run, args=[master_addr, table]))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return

if __name__ == '__main__':
    # 获取master地址
    master_addresses = ConfigManager().get_client_conf("sp", "kudu")['master_address']
    # 获取所有的表
    cmd = 'sp_kudu table list {master_addresses}'.format(master_addresses=master_addresses)
    res = BalanceTool().run_cmd(cmd)
    tables_list = res['stdout'].strip().split('\n')
    for table in tables_list:
        print("Begin to rebalance table: %s" % table)
        # 用于线程间通信：
        # 0表示没有超时，搜索任务可以继续执行，
        # 1表示超时，搜索任务需要取消，
        # 2表示搜索任务完成，计时器任务需要关闭
        global timeout_signal
        timeout_signal = 0
        balance_table(master_addresses, table)
    sys.exit(1)
