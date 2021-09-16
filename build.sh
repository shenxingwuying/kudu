set -ex

rm -rf thirdparty
tar xzf /opt/kudu/thirdparty.tgz

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/thirdparty/installed/uninstrumented/lib/

INSTALL_DIR=/opt/kudu/installed

if [ ! -d build/release ]; then
  mkdir -p build/release
fi
cd build/release

mkdir -p $INSTALL_DIR
../../build-support/enable_devtoolset.sh \
  ../../thirdparty/installed/common/bin/cmake \
  -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
  -DCMAKE_BUILD_TYPE=release ../..

PARALLEL=$(grep -c processor /proc/cpuinfo)
make -j${PARALLEL}
make install

mkdir -p $INSTALL_DIR/lib/kudu
cp -r ../../www $INSTALL_DIR/lib/kudu/

strip --remove-section=.symtab $INSTALL_DIR/bin/kudu
strip --remove-section=.strtab $INSTALL_DIR/bin/kudu

strip --remove-section=.symtab $INSTALL_DIR/sbin/kudu-master
strip --remove-section=.strtab $INSTALL_DIR/sbin/kudu-master

strip --remove-section=.symtab $INSTALL_DIR/sbin/kudu-tserver
strip --remove-section=.strtab $INSTALL_DIR/sbin/kudu-tserver

strip --remove-section=.symtab $INSTALL_DIR/lib64/libkudu_client.so.0.1.0
strip --remove-section=.strtab $INSTALL_DIR/lib64/libkudu_client.so.0.1.0

cat << EOF > $INSTALL_DIR/files.yml
files: ['kudu/bin', 'kudu/include', 'kudu/lib64', 'kudu/sbin', 'kudu/share', 'kudu/lib']
EOF
