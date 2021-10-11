#!/bin/bash

set -x
set -e

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

# 根据 soku_parcel 在 /opt/soku 生成的 parcel 目录，上传最终的 parcel 文件
script_path=`dirname $0`
bin_path='/opt/kudu'
cdh_parcel=${bin_path}/'cdh_parcel'

pub_report=${script_path}/../_dragon/pub/publish_report.yml
version=`cat ${pub_report} | sed -n 's/ *archive_name: cdh_parcel-//p' | awk -F '-' '{print $1}'`
level=`cat ${pub_report} | sed -n 's/ *level: //p' | head -1`

#去首尾空格
version=`echo ${version}`
full_version='KUDU_SENSORS_DATA-'${version}'-cdh5.12.1.p0'
parcel_name=${full_version}-${od}.parcel

# 如果是 develop 版本，则打上 develop 标记
if [ "x"${level} = "xdevelop" ]; then
  full_develop_version=${full_version}"."${level}
  mv ${bin_path}/${full_version} ${bin_path}/${full_develop_version}
  cp -f ${script_path}/../parcel-meta/meta/parcel.json ${bin_path}/${full_develop_version}/meta
  # 生成 meta 版本
  sed -i "s/{autogen_version}/${version}/g" ${bin_path}/${full_develop_version}/meta/parcel.json
  sed -i "s/{od}/${od}/g" ${bin_path}/${full_develop_version}/meta/kudu_env.sh
  sed -i "s/{level}/\.develop/g" ${bin_path}/${full_develop_version}/meta/parcel.json
  sed -i "s/{autogen_version}/${version}/g" ${bin_path}/${full_develop_version}/meta/kudu_env.sh
  # 生成 parcel
  parcel_name=${full_develop_version}-${od}.parcel
  tar czf ${parcel_name} -C ${bin_path}/ ${full_develop_version}
  sha1sum ${parcel_name} | awk '{print $1}' > ${parcel_name}.sha
else
  cp ${cdh_parcel}/${parcel_name} ${script_path}
  cp ${cdh_parcel}/${parcel_name}.sha ${script_path}
fi

# 生成 manifest 文件
python3 make_manifest.py .
mv manifest.json ${parcel_name}.manifest.json

#上传 parcel
jfrog c add --url=https://jfrog-internal.sensorsdata.cn/ --password=AKCp8ii9JXM1owdsqX7udFrZX26fisBjxVV5vsyTRFgyCDuXc7nuomHCk9BMJhb8ukgtU4WH2 --interactive=false
jfrog rt u ${parcel_name}.sha dragon-cdh/sdp/kudu/${level}/${od}/${parcel_name}.sha
jfrog rt u ${parcel_name} dragon-cdh/sdp/kudu/${level}/${od}/${parcel_name}
jfrog rt u ${parcel_name}.manifest.json dragon-cdh/sdp/kudu/${level}/${od}/${parcel_name}.manifest.json
