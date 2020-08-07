set -ex

# install 路径
mkdir -p /data/opt/kudu

# 代码路径
cd /data/code/kudu

# build 路径
mkdir -p build/release
cd build/release

# 编译 & 安装
../../build-support/enable_devtoolset.sh \
  ../../thirdparty/installed/common/bin/cmake \
  -DCMAKE_INSTALL_PREFIX=/data/opt/kudu \
  -DCMAKE_BUILD_TYPE=release ../..

make -j6
make install

# 二进制瘦身
strip --remove-section=.symtab /data/opt/kudu/bin/kudu
strip --remove-section=.strtab /data/opt/kudu/bin/kudu

strip --remove-section=.symtab /data/opt/kudu/sbin/kudu-master
strip --remove-section=.strtab /data/opt/kudu/sbin/kudu-master

strip --remove-section=.symtab /data/opt/kudu/sbin/kudu-tserver
strip --remove-section=.strtab /data/opt/kudu/sbin/kudu-tserver

strip --remove-section=.symtab /data/opt/kudu/lib64/libkudu_client.so.0.1.0
strip --remove-section=.strtab /data/opt/kudu/lib64/libkudu_client.so.0.1.0
