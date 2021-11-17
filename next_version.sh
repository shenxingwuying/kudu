#!/bin/bash

version=

if [ $# -gt 0 ]; then
  version=$1
else
  echo "error, lost of args"
fi

base_path=$(cd "$(dirname "$0")"; pwd)

source_version=`cat ${base_path}/build-tools/package.yml | grep version | sed 's/.*\"\([0-9]*\.[0-9]*\.[0-9]*\)\"/\1/g' | head -1`
release_version=`echo ${version} |sed 's/^\([0-9]*\.[0-9]*\.[0-9]*\)\.[0-9]/\1/g'`

next_version=
if [ "x"${source_version} != "x"${release_version} ]; then
  echo "Version is mismatch, no need update to next version"
  exit 0
fi
#next_version=`echo ${source_version} | sed 's/^\([0-9]*\.[0-9]*\)\.[0-9]/\1/g'`.`echo ${version_version} | sed 's/^[0-9]*\.[0-9]*\.\([0-9]*\)/\1/g' `
next_version=`echo ${source_version} | sed 's/^\([0-9]*\.[0-9]*\)\.[0-9]*/\1/g'`.`expr $(echo ${source_version} | sed 's/^[0-9]*\.[0-9]*\.\([0-9]*\)/\1/g') + 1`
sed -i "s/${source_version}/${next_version}/g" ${base_path}/build-tools/package.yml
sed -i "s/${source_version}/${next_version}/g" ${base_path}/bite-kudu/build-tools/package.yml

git config --global user.email dengke@sensorsdata.cn
git config --global user.name dengke
git add ${base_path}/build-tools/package.yml
git add ${base_path}/bite-kudu/build-tools/package.yml
git commit -m "[KUDU-15] update develop version to ${next_version}"
git push origin HEAD:refs/heads/dev/automate/${next_version}
git config --add gitlab.url "http://gitlab.sensorsdata.cn/"
git config --add gitlab.token "AF_sJwd2d8eUXxxvAvsp"
lab merge-request -b origin/dev/automate/${next_version} -t origin/develop/1.14.x -m "[KUDU-15] update develop version to ${next_version}" -a zhangqianqiong