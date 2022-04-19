#!/bin/bash

set -ex

export THIRDPARTY_DIR=/opt/code/kudu/thirdparty
export NO_REBUILD_THIRDPARTY=1

INSTALL_DIR=/opt/collector/installed
CODE_ROOT=$(cd "$(dirname "$0")"; pwd)

if [ ! -d build/collector ]; then
  mkdir -p build/collector
fi
cd build/collector
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

# collector模块下不需要kudu-master/kudu-tserver二进制文件
rm $INSTALL_DIR/sbin/kudu-master
rm $INSTALL_DIR/sbin/kudu-tserver
rm $INSTALL_DIR/lib64/libkudu_client.so.0.1.0

strip --remove-section=.symtab $INSTALL_DIR/bin/kudu
strip --remove-section=.strtab $INSTALL_DIR/bin/kudu

strip --remove-section=.symtab $INSTALL_DIR/sbin/kudu-collector
strip --remove-section=.strtab $INSTALL_DIR/sbin/kudu-collector

cat << EOF > $INSTALL_DIR/files.yml
files: ['kudu/bin', 'kudu/include', 'kudu/lib64', 'kudu/sbin', 'kudu/share', 'kudu/lib']
EOF
