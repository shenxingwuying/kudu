#!/bin/bash
# 本脚本用于收集kudu镜像的制品文件
# 执行前先清空可能存在的shim目录和kudu目录，再执行拷贝

set -e
set -x

INSTALL_DIR=./binary/installed
target_dir=./build-tools/dockerfiles/kudu
# 先清空可能存在的shim和kudu目录，避免对后续拷贝造成干扰
rm -rf ${target_dir}/shim
rm -rf ${target_dir}/admintools
rm -rf ${target_dir}/kudu
cp -rf shim ${target_dir}
cp -rf admintools ${target_dir}
cp -rf ${INSTALL_DIR} ${target_dir}
mv ${target_dir}/installed ${target_dir}/kudu
