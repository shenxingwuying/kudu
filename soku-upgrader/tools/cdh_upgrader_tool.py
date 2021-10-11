import os
import sys
import time
import datetime
import traceback

sys.path.append(os.path.dirname(__file__))
from base_upgrader_tool import BaseUpgraderTool
from hyperion_utils import shell_utils
from hyperion_client.deploy_topo import DeployTopo
import utils.sa_cm_api


class CdhUpgraderTool(BaseUpgraderTool):
    def __init__(self, logger, package_dir):
        super().__init__(logger, package_dir)
        self.api = utils.sa_cm_api.SaCmAPI(logger=self.logger)
        self.parcel_repo_dir = os.path.abspath(os.path.join(self.api.conf['parcel_path'], '..', 'parcel-repo'))
        self.parcel_info_dict = {}
        self.parcel_files = []
        self.parcel_dir = os.path.join(self.package_dir, 'cdh_parcel')
        for file in os.listdir(self.package_dir):
            if file.startswith('cdh_parcel') and file.endswith('.tar'):
                cdh_parcel_tar = os.path.join(self.package_dir, file)
                # 单独解压出来获取 parcel 文件
                shell_utils.check_call(f"tar -xf {cdh_parcel_tar}"
                                       f" -C {self.package_dir}")
                self.parcel_files = os.listdir(self.parcel_dir)
        self.hosts = DeployTopo().get_all_host_list()
        for f in self.parcel_files:
            if f.lower().startswith('kudu') and f.lower().endswith('parcel'):
                fields = f.split('-')
                self.parcel_info_dict = {
                    'name': fields[0],
                    'version': fields[1] + '-' + fields[2],
                    'path': os.path.join(self.parcel_dir, f),
                    'file': f,
                }
        self.logger.info('parcel: %s' % self.parcel_info_dict)

    def get_parcel_next_status(self, parcel_info):
        '''根据当前的状态, 决定当前动作'''
        """
        unknown->AVAILABLE_REMOTELY->DOWNLOADED->DISTRIBUTING->DISTRIBUTED->ACTIVATING->ACTIVATED
        """
        status_list = ["unknown", "AVAILABLE_REMOTELY", "DOWNLOADED", "DISTRIBUTING", "DISTRIBUTED", "ACTIVATING", "ACTIVATED"]
        if parcel_info['status'] not in status_list:
            raise Exception('unknown status %s' % parcel_info['status'])
        if parcel_info['status'] == 'DOWNLOADED':
            self.api.post_parcel_command(parcel_info['name'], parcel_info['version'], 'startDistribution')
        elif parcel_info['status'] == 'DISTRIBUTED':
            self.api.post_parcel_command(parcel_info['name'], parcel_info['version'], 'activate')
        parcel_info['finished'] = True if parcel_info['status'] == 'ACTIVATED' else False
        self.logger.info('%(name)s status: %(status)s' % parcel_info)

    def check_update_parcel_done(self):
        """检查parcel是否升级完毕"""
        if not self.parcel_info_dict['finished']:
            return False
        return True

    def find_relative_services(self):
        # 获取相关的服务
        services = {}
        r = self.api.get_parcel_usage()
        self.logger.info('get_parcel_usage: %s' % r)

        for h in r['racks'][0]['hosts']:
            for role in h['roles']:
                if role['roleRef']['serviceName'] in services:
                    continue
                for parcel_ref in role['parcelRefs']:
                    if parcel_ref['parcelName'].lower().startswith('kudu'):
                        services[role['roleRef']['serviceName']] = parcel_ref['parcelName']
        self.logger.info('relative services: %s' % services)
        return services

    def wait_service_done(self, timeout=600):
        '''确认服务正常 注意这里的服务正常是语义检查'''
        start = datetime.datetime.now()
        while (datetime.datetime.now() - start).total_seconds() < timeout:
            try:
                # 默认服务起来就ok了
                status = self.api.get_service_status('kudu')
                self.logger.info('kudu service %s health %s' % (status['serviceState'], status['healthSummary']))
                if status['serviceState'] == 'STARTED':
                    return True
            except Exception:
                self.logger.debug('ignore exception')
                self.logger.debug(traceback.format_exc())
                self.logger.info('waiting for kudu ready...')
            time.sleep(1)
        else:
            raise Exception('service not ready after %d seconds!' % timeout)

    def distribute_parcel(self):
        for f in self.parcel_files:
            if not (f.lower().endswith('parcel') or f.lower().endswith('parcel.sha')):
                continue
            src = os.path.join(self.parcel_dir, f)
            dst = os.path.join(self.parcel_repo_dir, f)
            if not os.path.isfile(dst):
                shell_utils.check_call('sudo cp %s %s' % (src, dst), self.logger.debug)
            else:
                self.logger.info('%s already exists' % dst)
            # 重启服务
        shell_utils.check_call('sudo setsid service cloudera-scm-server restart', self.logger.debug)
        shell_utils.check_call('sudo setsid service cloudera-scm-agent restart', self.logger.debug)
        self.logger.info('restart cloudera, waiting for scm server ready')
        self.api.waiting_service_ready()
        self.wait_parcel_activated()

    def wait_scm_server(self, timeout=120):
        # 单机版启动 scm-server
        need_start = True if len(self.hosts) == 1 else False
        # 等待 scm server 启动
        self.logger.info('waiting for scm server ready')
        self.api.waiting_service_ready(need_start=need_start)
        # 等待现有 parcel 已经变成激活状态，如果没有一个激活状态的parcel说明系统有问题
        self.wait_parcel_activated()

    def wait_parcel_activated(self, timeout=120):
        # 等待现有 parcel 已经变成激活状态，如果没有一个激活状态的parcel说明系统有问题
        self.logger.info('waiting for parcel status activated')
        start = datetime.datetime.now()
        while (datetime.datetime.now() - start).total_seconds() < timeout:
            self.logger.info("wait init parcel status activated")
            response = self.api.get_parcel_info()
            for item in response['items']:
                self.logger.info('item[\'product\'] = %s' % item['product'])
                if item['product'] in [self.parcel_info_dict['name']] and item['stage'] == 'ACTIVATED':
                    self.logger.info("init parcel status activated done")
                    return True
            self.logger.info("wait init parcel status:%s" % response)
            time.sleep(1)
        else:
            raise Exception(
                'parcel status not activated  after {} seconds! status:{}'.format(timeout, response['items']))

    def single_host_update(self):
        # 检查 scm server 可以升级
        self.wait_scm_server()
        if not self.need_update_parcel():
            self.logger.info('parcel is latest, no need to update')
            return
        # 1. 记录初始状态
        initial_status = []
        # 获取要升级的parcel版本的状态，这个时候可能已经是新的了，也可能还没有这个 parcel
        status = self.api.get_parcel_status(self.parcel_info_dict['name'], self.parcel_info_dict['version'])
        initial_status.append(status)
        self.parcel_info_dict['status'] = status
        # 包含以下状态需要先分发 parcel 到 parcel repo
        if 'unknown' in initial_status or 'DOWNLOADED' in initial_status or 'AVAILABLE_REMOTELY' in initial_status:
            self.logger.info('distributing parcel')
            self.distribute_parcel()
        self.logger.info('waiting for parcel activated')
        # 2. 一步步直到服务激活
        self.get_parcel_next_status(self.parcel_info_dict)
        if self.parcel_info_dict['finished']:
            self.logger.info('kudu parcels are latest and status is finished')
            return
        while True:
            if self.parcel_info_dict['finished']:
                continue
            # 获取当前最新的状态
            new_status = self.api.get_parcel_status(self.parcel_info_dict['name'], self.parcel_info_dict['version'])
            if self.parcel_info_dict['status'] != new_status:
                self.parcel_info_dict['status'] = new_status
                self.get_parcel_next_status(self.parcel_info_dict)
            if self.check_update_parcel_done():
                self.logger.info('update parcel done!')
                break
            time.sleep(1)
        # 3. 重启服务
        for name in self.find_relative_services():
            self.api.post_service_command(name, 'restart', wait=True)
        # 4. 确认服务启动正常
        self.wait_service_done(timeout=30 * 60)

    def update(self):
        # 只有等于 cm host 的机器才升级
        if self.my_host == self.api.cm_host:
            self.single_host_update()
            # 单机版停止 scm-server
            if len(self.hosts) == 1:
                cmd = 'sudo service cloudera-scm-server stop'
                shell_utils.check_call(cmd, self.logger.debug)
        else:
            self.logger.info("not cm_host %s, skip" % self.api.cm_host)

    def need_update_parcel(self):
        '''判断一下是否需要更新parcel'''
        # kudu parcel 包有下面两种格式
        # KUDU_SENSORS_DATA-1.12.3-1.cdh5.12.1.p0.18-el7.parcel
        # KUDU-1.6.0-1.cdh5.12.1.p0.10-el6.parcel
        local_full_version = self.parcel_info_dict['version']
        remote_full_version_list = self.api.get_parcel_version(self.parcel_info_dict['name'], 'ACTIVATED')
        if len(remote_full_version_list) != 1:
            raise Exception('invalid ACTIVATED parcel version! name: {}, version: {}'
                            .format(self.parcel_info_dict['name'], remote_full_version_list))
        remote_full_version = remote_full_version_list[0]
        return self.compare_version(local_full_version, remote_full_version)
