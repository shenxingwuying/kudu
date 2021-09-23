FROM jfrog-internal.sensorsdata.cn/docker/com.sensorsdata.dragon/bite:0.3.0.159

RUN yum install -y autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
      cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
      krb5-server krb5-workstation libtool make openssl-devel patch \
      pkgconfig redhat-lsb-core rsync unzip vim-common which

RUN yum install -y python-argparse lsof scl-utils
RUN yum install -y centos-release-scl-rh
RUN yum install -y devtoolset-8

RUN mkdir -p /opt/kudu_code
WORKDIR /opt/kudu_code
RUN git clone http://cd-token:iznQUqBe4hu5kNrdgQK5@gitlab.internal.sensorsdata.cn/sensors-analytics/kudu.git

WORKDIR /opt/kudu_code/kudu
RUN git checkout infinity/branch-1.14.x
RUN build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh
RUN mkdir -p /opt/kudu/ && mv thirdparty /opt/kudu/
RUN rm -rf /opt/kudu_code
