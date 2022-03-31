#!/bin/bash

set -x
set -e

version=
while [ $# -ge 2 ]; do
    case "$1" in
        --version) version=$2; shift 2;;
        *) echo "unknown parameter $1."; exit 1; break;;
    esac
done

os_version=$(cat /etc/centos-release | sed 's/.*release //g' | awk '{print $1}' | awk -F. '{print $1}')
od=el7
if [ $os_version -eq 6 ]; then
    od=el6
elif [ $os_version -eq 7 ]; then
    od=el7
else
    echo "os: $os_version not support!"
    exit 1
fi

script_path=$(cd "$(dirname "$0")"; pwd)
bin_path='/opt/kudu'

if [ ! -d ${script_path}/../_dragon/build/kudu-${od} ]; then
    # kudu 未编译，先执行编译流程
    sh ${script_path}/../build.sh
fi

#去首尾空格
version=`echo ${version}`
full_version='KUDU_SENSORS_DATA-'${version}'-cdh5.12.1.p0'
cdh_parcel=${bin_path}/'cdh_parcel'

cp -rf ${bin_path}/installed ${bin_path}/${full_version}
cp -rf ${script_path}/../parcel-meta/meta ${bin_path}/${full_version}/

# Fix cdh --superuser_acl=sa_cluster,root,kudu for compatibility
mkdir -p ${bin_path}/${full_version}/lib/kudu/
cp -rf ${script_path}/../parcel-meta/sbin-release ${bin_path}/${full_version}/lib/kudu/

pushd ${bin_path}/${full_version}/lib/kudu/
ln -s sbin-release sbin
popd

#生成 meta 版本
sed -i "s/{autogen_version}/${version}/g" ${bin_path}/${full_version}/meta/parcel.json
if [ "x"${level} = "xdevelop" ]; then
  sed -i "s/{level}/\.develop/g" ${bin_path}/${full_version}/meta/parcel.json
else
  sed -i "s/{level}//g" ${bin_path}/${full_version}/meta/parcel.json
fi
sed -i "s/{od}/${od}/g" ${bin_path}/${full_version}/meta/kudu_env.sh
sed -i "s/{autogen_version}/${version}/g" ${bin_path}/${full_version}/meta/kudu_env.sh

#生成 parcel
parcel_name=${full_version}-${od}.parcel
tar czf ${parcel_name} -C ${bin_path}/ ${full_version}
sha1sum ${parcel_name} |awk '{print $1}' > ${parcel_name}.sha
python3 ${script_path}/make_manifest.py .
mv manifest.json ${parcel_name}.manifest.json

if [ ! -d ${cdh_parcel} ]; then
    mkdir -p ${cdh_parcel}
else
    rm -f ${cdh_parcel}/*
fi
mv ${parcel_name} ${cdh_parcel}
mv ${parcel_name}.sha ${cdh_parcel}
