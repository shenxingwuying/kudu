#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys

from hyperion_utils.shell_utils import check_call
import utils.sa_utils
from hyperion_client.deploy_info import DeployInfo
sys.path.append(os.environ['SENSORS_PLATFORM_HOME'])
from construction_blueprint.installer_constants import HadoopDistributionType

# 超时时间,3小时
TIMEOUT = 10800


def main():
    """主函数是由scheduler调度的定时任务"""
    logger = utils.sa_utils.init_admintool_logger('soku', 'maintenance')
    logger.info('Please check log in {}'.format(
        os.path.join(os.environ.get('SENSORS_SOKU_HOME'), 'logs', 'admintools', 'maintenance.log')))
    # 暂时只处理cdh和mothership环境
    if ((DeployInfo().get_hadoop_distribution() == HadoopDistributionType.MOTHERSHIP)
       or (DeployInfo().get_hadoop_distribution() == HadoopDistributionType.CLOUDERA)):
        cmd = 'sokuadmin leader_balance'
        check_call(cmd, logger.debug, timeout=TIMEOUT)
    else:
        # 混部场景，暂时不处理
        logger.info('hadoop distribution type is mix, no need to do maintenance work')


if __name__ == '__main__':
    main()
