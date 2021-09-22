# bite 最新版：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=118984014
FROM jfrog-internal.sensorsdata.cn/docker/com.sensorsdata.dragon/bite_el6:0.3.0.41

RUN yum install -y autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
      cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
      krb5-server krb5-workstation libtool make openssl-devel patch \
      pkgconfig redhat-lsb-core rsync unzip vim-common which

RUN yum install -y python-argparse lsof scl-utils
RUN yum install -y centos-release-scl-rh
RUN yum install -y devtoolset-8

# copy kudu code from local to the /opt/kudu_code, we need the absolutely path
RUN rm -rf /opt/kudu_code && rsync -aP -r ./ /opt/kudu_code

WORKDIR /opt/kudu_code
RUN build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh
RUN mkdir -p /opt/kudu && mv thirdparty /opt/kudu/
