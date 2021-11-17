#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys
import subprocess
import time

from collections import defaultdict

import utils.sa_utils
import utils.shell_wrapper

from hyperion_client.config_manager import ConfigManager
from hyperion_client.deploy_topo import DeployTopo

SOKU_TOOL_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'soku_tool')
if SOKU_TOOL_ROOT_PATH not in sys.path:
    sys.path.append(SOKU_TOOL_ROOT_PATH)
sys.path.append(os.path.join(os.environ['SENSORS_SOKU_HOME'], 'soku_tool', 'tools'))
from base_tool import BaseTool

class BalanceTool(BaseTool):
    example_doc = '''
soku_tool banlance start # 开始balance
'''
    def _is_standalone(self):
        """简单判断是否单机"""
        master_addresses = ConfigManager().get_client_conf("sp", "kudu")['master_address']
        get_ts_list_cmd = 'kudu tserver list %s -columns=http_addresses -format=space' % \
                      master_addresses
        res = utils.shell_wrapper.run_cmd(get_ts_list_cmd, self.logger.info)
        if res['ret'] != 0:
            raise Exception('Unable to call kudu tserver list cmd. %s' % res['stderr'])
        ts_cnt = len(res['stdout'].strip().split('\n'))
        return  ts_cnt == 1

    def init_parser(self, parser):
        return 0

    def do(self, args):
        self.logger.debug(args)
        if self._is_standalone():
            self.logger.info("This cluster is standalone, no need do leader rebalance. Exit!!!")
            sys.exit(0)
        master_addresses = ConfigManager().get_client_conf("sp", "kudu")['master_address']
        get_table_cmd = 'kudu table list {master_addresses}'.format(master_addresses=master_addresses)
        res = utils.shell_wrapper.run_cmd(get_table_cmd, self.logger.info)
        if res['ret'] != 0:
            raise Exception('Unable to call kudu table list cmd. %s' % res['stderr'])
        table_str = res['stdout'].strip().split('\n')
        for table in table_str:
            self.solution(master_addresses, table)
        self.logger.info('NOTE: cluster has rebalance done, exit!!!')
        sys.exit(0)

    def ksck(self, master_addresses):
        ksck_cmd = 'kudu cluster ksck %s -ksck_format=plain_full' % master_addresses
        ksck_output = utils.shell_wrapper.run_cmd(ksck_cmd, self.logger.info)
        if ksck_output['ret'] != 0:
            raise Exception('Unable to call kudu cluster ksck cmd. %s' % res['stderr'])
        ksck_lines = ksck_output['stdout'].strip().split('\n')
        return ksck_lines

    def scheduling_uno_op(self, master_addresses, op):
        tablet, leader = op
        step_down_cmd = 'kudu tablet leader_step_down %s %s -new_leader_uuid=%s' % (master_addresses, tablet, leader)
        utils.shell_wrapper.run_cmd(step_down_cmd, self.logger.info)

    def parse_ksck(self, ksck_lines, table_filter):
        idx = 0
        ts_map = defaultdict(list)
        tablet_tuple_list = list()
        while idx < len(ksck_lines):
            line = ksck_lines[idx]
            tablet_id, table = '', ''
            # e.g. Tablet 288b5b776eb84f0f9b1f32163be5870b of table 'profile_wos_p2' is healthy.
            if 'Tablet' in line and 'of table' in line:
                tablet_id = line.split(' ')[1]
                table = line.split(' ')[4][1:-1]
                idx += 1
            else:
                idx += 1
                continue
            if  table not in table_filter:
                continue
            leader_ts = ''
            all_ts_of_tablet = []
            ts_id = ''
            # To parse the tablet ts mapping info e.g.
            #   a1cbc1e51e6e43d9be09bea38c3fade3 (debugboxreset1775x3.sa:7050): RUNNING [LEADER]
            #   e1919bbab5f544128aab6215451583a9 (debugboxreset1775x2.sa:7050): RUNNING
            #   b5d3cd51688a4af1a25a6d5c9ba4c840 (debugboxreset1775x1.sa:7050): RUNNING
            while ksck_lines[idx].startswith('  '):
                if not 'RUNNING' in ksck_lines[idx] or 'NONVOTER' in ksck_lines[idx]:
                    idx += 1
                    continue
                ts_id = ksck_lines[idx].strip().split(' ')[0]
                all_ts_of_tablet.append(ts_id)
                ts_map[ts_id] # access make all ts is in map
                if ksck_lines[idx].endswith('[LEADER]'):
                    ts_map[ts_id].append(tablet_id)
                    leader_ts = ts_id
                idx += 1
            if len(all_ts_of_tablet) != 3:
                del ts_map[ts_id]
            else:
                tablet_tuple_list.append((tablet_id, leader_ts, all_ts_of_tablet))
        return tablet_tuple_list, ts_map

    def cal_mean(self, ts_map):
        all_cnt = sum([len(_) for _ in ts_map.values()])
        ts_cnt = len(ts_map)
        mean_cnt = all_cnt / ts_cnt
        return mean_cnt

    def eval_score(self, ts_map):
        # true -> balanced
        mean_cnt = self.cal_mean(ts_map)
        score = sum([((len(_)) - mean_cnt) ** 2 for _ in ts_map.values()])
        return score, score < 1

    def print_skew_info(self, ts_map):
        mean_cnt = self.cal_mean(ts_map)
        for ts, v in ts_map.items():
            skew_value = len(v) - mean_cnt
            if abs(skew_value) < 1: # balanced
                self.logger.info('ts %s has %d avg is %f' % (ts, len(v), mean_cnt))
                continue
            elif skew_value > 0:
                self.logger.info('ts %s execceds %f' % (ts, skew_value))
            else:
                self.logger.info('ts %s behinds %f' % (ts, skew_value))

    reached = False
    best_op = [float("inf"), []]
    # op = (tablet, leader)

    def dfs(self, tablet_tuple_list, ops, leader_map, mean_cnt):
        sorted_ts_list = sorted(leader_map.items(), key=lambda x:len(x[1]))
        if self.reached:
            return
        if len(tablet_tuple_list) == 0:
            score, _ = self.eval_score(leader_map)
            if score < self.best_op[0]:
                self.best_op[0], self.best_op[1] = score, ops[::] # deep clone into result
                if score < 1:
                    self.reached = True
            return
        tablet_id, leader, ts_list = tablet_tuple_list.pop()
        for ts, tablets in sorted_ts_list:
            if ts in ts_list:
                leader_map[ts].append(tablet_id)
                if leader != ts:
                    ops.append((tablet_id, ts))
                self.dfs(tablet_tuple_list, ops[::], leader_map, mean_cnt)
                if leader != ts:
                    ops.pop()
                leader_map[ts].remove(tablet_id)

    def solution(self, master_addresses, table_filter):
        ksck_output = self.ksck(master_addresses)
        tablet_tuple_list, ts_map = self.parse_ksck(ksck_output, table_filter)
        score, balanced = self.eval_score(ts_map)
        self.logger.info('current skew info')
        self.print_skew_info(ts_map)
        if balanced:
            self.logger.info('table %s was rebalanced' % table_filter)
            return 0
        else:
            self.logger.info('looking for solution')
            leader_map = defaultdict(list)
            for ts in ts_map.keys():
                leader_map[ts] # just access and gen default
            self.dfs(tablet_tuple_list, [], leader_map, self.cal_mean(ts_map))
            self.logger.info(self.best_op[0])
            self.logger.info(self.best_op[1])
            for op in self.best_op[1]:
                self.scheduling_uno_op(master_addresses, op)
            try_time = 0
            has_to_try = True
            self.logger.info('gathering info')
            while try_time < 5 and has_to_try:
                has_to_try = not self.try_and_gen_score(master_addresses)
                try_time += 1
            return 1 if has_to_try else 0

    def try_and_gen_score(self, master_addresses):
        try:
            time.sleep(5)
            self.logger.info('waiting for skew info after gen')
            ksck_output = self.ksck(master_addresses)
            tablet_tuple_list, ts_map = self.parse_ksck(ksck_output, table_filter)
            score, balanced = self.eval_score(ts_map)
            self.logger.info('after rebalance skew map')
            self.print_skew_info(ts_map)
            return score < 1
        except:
            self.logger.info('cluster not rebalance done, waiting')
            return False