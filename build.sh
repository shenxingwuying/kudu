#!/bin/bash

set -ex

export THIRDPARTY_DIR=/opt/code/kudu/thirdparty
export NO_REBUILD_THIRDPARTY=1

INSTALL_DIR=/opt/kudu/installed
CODE_ROOT=$(cd "$(dirname "$0")"; pwd)

if [ -d ${CODE_ROOT}/_dragon/build/cdh_parcel-${os_tag} ]; then
    # 打 parcel 包已经编译过了，正常退出
    exit 0
fi

if [ -d ${CODE_ROOT}/_dragon/build/kudu-${os_tag} ]; then
    # 打 kudu 包已经编译过了，正常退出
    exit 0
fi

if [ -d ${CODE_ROOT}/_dragon/build/collector-${os_tag} ]; then
    # 打 collector 包已经编译过了，正常退出
    exit 0
fi

if [ -d ${CODE_ROOT}/_dragon/build/soku_tool-${os_tag} ]; then
    # 打 soku_tool 包已经编译过了，正常退出
    exit 0
fi

if [ -d ${CODE_ROOT}/_dragon/build/kudu_guidance-${os_tag} ]; then
    # 打 kudu_guidance 包已经编译过了，正常退出
    exit 0
fi

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

cd ${CODE_ROOT}/java
./gradlew :kudu-subprocess:jar
cp ${CODE_ROOT}/java/kudu-subprocess/build/libs/kudu-subprocess-*-SA.jar $INSTALL_DIR/bin/
mv $INSTALL_DIR/bin/kudu-subprocess-*-SA.jar  $INSTALL_DIR/bin/kudu-subprocess.jar

cat << EOF > $INSTALL_DIR/files.yml
files: ['kudu/bin', 'kudu/include', 'kudu/lib64', 'kudu/sbin', 'kudu/share', 'kudu/lib']
EOF
