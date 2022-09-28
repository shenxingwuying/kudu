#!/bin/sh
#
# @description:
# 独立部署的kudu-collector的启动脚本 参数是角色名 collector
set -xe
role=collector

SENSORS_SOKU_HOME=${SENSORS_PLATFORM_HOME}/../soku
COLLECTOR_HOME=${SENSORS_SOKU_HOME}/collector
COLLECTOR_LOG_HOME=${SENSORS_SOKU_HOME}/logs/${role}
COLLECTOR_CONF_HOME=${SENSORS_SOKU_HOME}/conf/kudu_${role}.gflagfile
export LD_LIBRARY_PATH="${COLLECTOR_HOME}/lib/kudu/sbin:${LD_LIBRARY_PATH}"

nohup ${COLLECTOR_HOME}/sbin/kudu-${role} --flagfile ${COLLECTOR_CONF_HOME} >> ${COLLECTOR_LOG_HOME}/kudu_${role}.out 2>> ${COLLECTOR_LOG_HOME}/kudu_${role}.err </dev/null &

sleep 1
# vim:expandtab shiftwidth=4 softtabstop=4
