#!/bin/bash
# 本脚本用于kudu二进制文件的prebuild操作的meta文件更新
# 该脚本对应Jenkins中soku的prebuild任务之后的脚本回调，修改时请注意配套修改
# 执行格式如下所示：
#
# sh build_binary_tar.sh ${timestamp} ${level}
#
# 执行示例如下所示：
#
# sh build_binary_tar_info.sh 2022-11-01-16-14-57 develop
#
# 生成的${timestamp}.manifest.json会被Jenkins job上传到jforg仓库，仓库路径如下：
# dragon-internal/inf/kudu/kudu_prebuild_binary/${level}/
# 其中${level}是打包的级别，取值范围为[develop, test]
# 同时更新仓库中最新的20次打包信息，并筛选出最近一次的打包制品，保存在同一目录下的package_index.json文件中


set -e
set -x

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

module=kudu
script_path=$(cd "$(dirname "$0")"; pwd)
# 非标准打包，不知道版本号，需要指定传入
version=
# 打包级别level用于区分文件路径
level=

if [ $# -gt 1 ]; then
  version=$1
  level=$2
elif [ $# -gt 0 ]; then
  version=$1
else
  echo "error, lost of args"
fi

package_name='kudu-'${version}
full_package_name='kudu-'${version}'.tar'

# 生成 manifest 文件
python3 ${script_path}/gen_manifest.py ${package_name} ${level}
mv manifest.json ${package_name}.manifest.json

# for test on local, 应该在job中配置,本地调试时替换为自己的key
#JFROG_PASSWORD=xxx
#jfrog c add --url=https://jfrog-internal.sensorsdata.cn/ --password=${JFROG_PASSWORD}  --interactive=false

# 上传 manifest.json
jfrog rt u ${package_name}.manifest.json dragon-internal/inf/${module}/kudu_prebuild_binary/${level}/${package_name}.manifest.json
rm ${package_name}.manifest.json

# 更新用于统计的package_index.json文件
jfrog rt dl dragon-internal/inf/${module}/kudu_prebuild_binary/${level}/*.manifest.json --flat
python3 ${script_path}/update_manifest.py .
jfrog rt u package_index.json dragon-internal/inf/${module}/kudu_prebuild_binary/${level}/package_index.json --flat
rm *.manifest.json
rm package_index.json
