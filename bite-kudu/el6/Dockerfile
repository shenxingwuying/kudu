# bite 最新版：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=118984014
FROM jfrog-internal.sensorsdata.cn/docker/com.sensorsdata.dragon/bite_el6:0.3.0.46

RUN yum install -y autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
      cyrus-sasl-plain flex gcc gcc-c++ gdb java-1.8.0-openjdk-devel \
      krb5-server krb5-workstation libtool make openssl-devel patch \
      pkgconfig redhat-lsb-core rsync unzip vim-common which

RUN yum install -y python-argparse lsof scl-utils
RUN yum install -y centos-release-scl-rh
RUN yum install -y devtoolset-8

WORKDIR /opt/code
RUN git clone http://cd-token:iznQUqBe4hu5kNrdgQK5@gitlab.sensorsdata.cn/sensors-analytics/kudu.git
WORKDIR /opt/code/kudu
# TODO(duyuqi). 
# 'release/1.14.2.x/feature.sensors_duplication' branch need change
# to a normal branch naming such as test/1.14.5.x or develop/1.14.5.x
# in the future.
RUN git checkout release/1.14.2.x/feature.sensors_duplication

RUN build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh all
