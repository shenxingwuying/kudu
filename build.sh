set -ex

kudu_source_code_absolute_path=/opt/kudu_code/kudu

# copy kudu code from local to the /opt/kudu_code, we need the absolutely path
rm -rf $kudu_source_code_absolute_path
mkdir -p $kudu_source_code_absolute_path
rsync -aP -r ./ $kudu_source_code_absolute_path

cd $kudu_source_code_absolute_path
rm -rf thirdparty
cp -r /opt/kudu/thirdparty thirdparty

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/thirdparty/installed/uninstrumented/lib/

INSTALL_DIR=/opt/kudu/installed

script_path=$(cd "$(dirname "$0")"; pwd)
if [ -d ${script_path}/_dragon/build/cdh_parcel-${os_tag} ]; then
    # 打 parcel 包已经编译过了，正常退出
    exit 0
fi

if [ -d ${script_path}/_dragon/build/kudu-${os_tag} ]; then
    # 打 kudu 包已经编译过了，正常退出
    exit 0
fi

if [ -d ${script_path}/_dragon/build/collector-${os_tag} ]; then
    # 打 collector 包已经编译过了，正常退出
    exit 0
fi

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
