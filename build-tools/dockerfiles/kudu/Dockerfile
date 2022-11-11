#FROM jfrog-internal.sensorsdata.cn/docker-test/com.sensorsdata.armada/el9_base_nojava:0.1.6.9
FROM jfrog-internal.sensorsdata.cn/docker-develop/com.sensorsdata.armada/el7_base_python:0.3.0.2

## 设置组件相关参数和环境变量
ARG PRODUCT_NAME=soku

USER sa_cluster
WORKDIR /home/sa_cluster/kudu

ENV SENSORS_SOKU_HOME /sensorsdata/main/program/$PRODUCT_NAME
ENV SENSORS_SOKU_LOG_DIR /sensorsdata/main/logs/$PRODUCT_NAME
ENV SENSORS_SOKU_RUNTIME_DIR /sensorsdata/main/runtime/${PRODUCT_NAME}
RUN mkdir -p ${SENSORS_SOKU_HOME} ${SENSORS_SOKU_LOG_DIR} ${SENSORS_SOKU_RUNTIME_DIR} ${SENSORS_SOKU_HOME}/kudu

## 拷贝主程序、相关脚本
COPY --chown=sa_cluster:sa_group kudu/ ${SENSORS_SOKU_HOME}/kudu/kudu
COPY --chown=sa_cluster:sa_group shim/ ${SENSORS_SOKU_HOME}/kudu_shim
COPY --chown=sa_cluster:sa_group admintools/ ${SENSORS_SOKU_HOME}/admintools

## 建立软链，适配宿主机的代码
RUN ln -s ${SENSORS_SOKU_HOME}/kudu/kudu ${SENSORS_SOKU_HOME}/kudu/kudu_collector && \
    ln -s ${SENSORS_SOKU_HOME}/kudu/kudu ${SENSORS_SOKU_HOME}/kudu/kudu_master && \
    ln -s ${SENSORS_SOKU_HOME}/kudu/kudu ${SENSORS_SOKU_HOME}/kudu/kudu_tserver

## 安装hyperion
RUN /opt/install_hyperion.sh
