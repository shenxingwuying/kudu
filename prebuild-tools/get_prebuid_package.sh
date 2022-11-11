#!/bin/bash
# 本脚本用于获取当前打包级别下最新的prebuild的kudu二进制tar包
# 运行环境为编译镜像
# 本地运行时可能会因为环境变量IS_DEVELOP缺失导致下载行为不符合预期，可通过手动设置解决
# 下载的二进制tar包会被解压到./binary目录下(docker场景下，目录识别存在限制)
# 由于历史原因，多个打包job可能会并发执行且无法控制先后，所以在脚本前做了存在性判断，旨在加快打包速度

set -ex

INSTALL_DIR=./binary/installed

if [ -d ${INSTALL_DIR}/bin/ ]; then
    # kudu 包已经下载过了，正常退出
    exit 0
fi

if [ -d ${INSTALL_DIR}/sbin/ ]; then
    # kudu 包已经下载过了，正常退出
    exit 0
fi

if [ -d ${INSTALL_DIR}/lib/ ]; then
    # kudu 包已经下载过了，正常退出
    exit 0
fi

if [ -d ${INSTALL_DIR}/lib64/ ]; then
    # kudu 包已经下载过了，正常退出
    exit 0
fi

if [ -d ${INSTALL_DIR}/include/ ]; then
    # kudu 包已经下载过了，正常退出
    exit 0
fi

if [ -d ${INSTALL_DIR}/share/ ]; then
    # kudu 包已经下载过了，正常退出
    exit 0
fi

module=kudu
script_path=$(cd "$(dirname "$0")"; pwd)
level=
if [ $IS_DEVELOP == 1 ]
then
    level="develop"
else
    level="test"
fi

# 清空当前路径上可能存在的package_index.json文件
rm -rf *package_index.json

# for test on local, 应该在job中配置,本地调试时替换为自己的key
#JFROG_PASSWORD=xxx
#jfrog c add --url=https://jfrog-internal.sensorsdata.cn/ --password=${JFROG_PASSWORD}  --interactive=false

# 下载用于统计的package_index.json文件
jfrog rt dl dragon-internal/inf/${module}/kudu_prebuild_binary/${level}/package_index.json --flat
latest_package=`python3 prebuild-tools/get_latest_package.py .`
version_tag=${latest_package: 5 :10}

package_url=https://jfrog-internal.sensorsdata.cn:443/artifactory/dragon-internal/inf/kudu/kudu_prebuild_binary/${level}/${version_tag}/${latest_package}

wget ${package_url}

if [ ! -d ./binary ]; then
    mkdir -p ./binary
else
    rm -rf ./binary/*
fi

tar -xvf ${latest_package}

if [ -d opt/kudu/installed ]; then
    mv opt/kudu/installed ./binary
fi
