#!/bin/bash
# 本脚本用于kudu二进制文件的prebuild操作，预先生成二进制的tar包
# 该脚本对应Jenkins中soku的prebuild任务，修改时请注意配套修改
# 由于是非标准打包，无法获取、生成版本号，故需要在Jenkins job调用执行脚本时传入一个时间字符串作为参数
# 执行格式如下所示：
#
# sh build_tar.sh ${timestamp}
#
# 执行示例如下所示：
#
# sh build_tar.sh 2022-11-01-15-12-56
#
# 该脚本执行完后tar的路径为/opt/kudu/kudu-${timestamp}.tar
# 生成的tar包会被Jenkins job上传到jforg仓库，仓库路径如下：
# dragon-internal/inf/kudu/kudu_prebuild_binary/${level}/
# 其中${level}是打包的级别，取值范围为[develop, test]

set -ex

export THIRDPARTY_DIR=/opt/code/kudu/thirdparty
export NO_REBUILD_THIRDPARTY=1

INSTALL_DIR=/opt/kudu/installed
CODE_ROOT=$(cd "$(dirname "$0")"; pwd)

if [ ! -d build/release ]; then
  mkdir -p build/release
fi
cd build/release
mkdir -p $INSTALL_DIR
${CODE_ROOT}/build-support/enable_devtoolset.sh \
  ${THIRDPARTY_DIR}/installed/common/bin/cmake \
  -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
  -DCMAKE_BUILD_TYPE=release \
  -DNO_TESTS=1 \
  ${CODE_ROOT}

PARALLEL=$(grep -c processor /proc/cpuinfo)
make -j${PARALLEL}
make install

mkdir -p $INSTALL_DIR/lib/kudu
cp -r ${CODE_ROOT}/www $INSTALL_DIR/lib/kudu/

strip --remove-section=.symtab $INSTALL_DIR/sbin/kudu
strip --remove-section=.strtab $INSTALL_DIR/sbin/kudu

strip --remove-section=.symtab $INSTALL_DIR/sbin/kudu-master
strip --remove-section=.strtab $INSTALL_DIR/sbin/kudu-master

strip --remove-section=.symtab $INSTALL_DIR/sbin/kudu-tserver
strip --remove-section=.strtab $INSTALL_DIR/sbin/kudu-tserver

strip --remove-section=.symtab $INSTALL_DIR/sbin/kudu-collector
strip --remove-section=.strtab $INSTALL_DIR/sbin/kudu-collector

strip --remove-section=.symtab $INSTALL_DIR/lib64/libkudu_client.so.0.1.0
strip --remove-section=.strtab $INSTALL_DIR/lib64/libkudu_client.so.0.1.0

cat << EOF > $INSTALL_DIR/files.yml
files: ['kudu/bin', 'kudu/include', 'kudu/lib64', 'kudu/sbin', 'kudu/share', 'kudu/lib']
EOF

if [ -e "${INSTALL_DIR}/sbin/kudu-master" ]; then
  tar cvf /opt/kudu/kudu-${1}.tar ${INSTALL_DIR}
fi
