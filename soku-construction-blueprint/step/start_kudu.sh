#!/bin/sh
#
# @description:
# 独立部署的kudu-collector的启动脚本 参数是角色名 collector
set -xe
if test $# -ne 1
then
    echo "usage: $0 <collector>"
    exit 1
fi
role=$1

SENSORS_SOKU_HOME=${SENSORS_PLATFORM_HOME}/../soku
COLLECTOR_HOME=${SENSORS_SOKU_HOME}/collector
COLLECTOR_LOG_HOME=${SENSORS_SOKU_HOME}/logs/collector
export LD_LIBRARY_PATH="${COLLECTOR_HOME}/lib/kudu/sbin:${LD_LIBRARY_PATH}"
cd ${COLLECTOR_HOME}
if [ ! -L "sbin/kudu" ];then
    cp bin/kudu sbin/kudu
fi
nohup ${COLLECTOR_HOME}/sbin/kudu-${role} --flagfile ../conf/kudu_${role}.gflagfile >> ${COLLECTOR_LOG_HOME}/kudu_${role}.out 2>> ${COLLECTOR_LOG_HOME}/kudu_${role}.err </dev/null &

sleep 1
# vim:expandtab shiftwidth=4 softtabstop=4
