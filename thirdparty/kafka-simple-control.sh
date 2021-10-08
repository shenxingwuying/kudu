#!/bin/bash

# a simple control for kafka standalone. for kudu duplicator itest

set -e

function usage() {
    echo "usage: =(install|start|stop|uninstall)"
}

if [[ -z $1 ]]; then
    echo "you should input an action"
    usage
    exit 1
fi

action=$1

kafka_package_name=kafka_2.13-3.1.0
kafka_package=$kafka_package_name.tgz

# download_url="https://dlcdn.apache.org/kafka/3.1.0/$kafka_package"
download_url="http://10.120.234.157:8088/$kafka_package"

brokers_list=localhost:9092
topic_name=kudu_profile_record_stream
kafka_target=itest-kafka-1279

function download_kafka() {
    pushd /tmp
    if [ x"$kafka_package" != x ] && [ -f $kafka_package ]; then
        echo "/tmp/$kafka_package has exist, skip download"
    else
        wget -q --tries=5 --timeout=30 $download_url -O $kafka_package --no-check-certificate
    fi
    if [ x"$kafka_target" != x ] && [ -d $kafka_target ]; then
        uninstall_kafka
    fi
    tar xzf $kafka_package
    mv $kafka_package_name $kafka_target
    popd
}

function start_kafka() {
    download_kafka
    pushd /tmp/$kafka_target
    # Start the ZooKeeper service
    # Note: Soon, ZooKeeper will no longer be required by Apache Kafka.
    bin/zookeeper-server-start.sh config/zookeeper.properties \
        &> /tmp/zookeeper.out </dev/null &

    # Start the Kafka broker service
    bin/kafka-server-start.sh config/server.properties \
        &> /tmp/kafka-server.out </dev/null &

    sleep 5 
    popd
}

function create_topic() {
    pushd /tmp/$kafka_target
    bin/kafka-topics.sh --create --topic $topic_name --bootstrap-server $brokers_list
    sleep 1
    bin/kafka-topics.sh --describe --topic $topic_name --bootstrap-server $brokers_list
    echo "create_topic success"
    popd
}

function test_write_and_read() {
    # Test kafka service ok? mannual confirm service

    pushd /tmp/$kafka_target
    # producer
    bin/kafka-console-producer.sh --topic $topic_name --bootstrap-server $brokers_list

    # consumer
    bin/kafka-console-consumer.sh --topic $topic_name --from-beginning --bootstrap-server $brokers_list
    popd
}

function stop_kafka() {
    ## TODO(duyuqi) should fix it.
    # stop server, 
    ps auxwf | grep kafka.Kafka | grep -v grep \
        | awk '{print $2}' | xargs -i kill -9 {}
    ps auxwf | grep org.apache.zookeeper.server.quorum.QuorumPeerMain \
        | grep -v grep | awk '{print $2}' | xargs -i kill -9 {}
    
    rm -rf /tmp/$kafka_target /tmp/zookeeper /tmp/kafka-logs
}

function uninstall_kafka() {
    stop_kafka
    clean_env
}

function clean_env() {
    rm -rf /tmp/$kafka_target /tmp/zookeeper /tmp/kafka-logs
}

case $action in 
    "install")
        download_kafka
        exit 0
        ;;
    "start")
        start_kafka
        create_topic
        exit 0
        ;;
    "stop")
        stop_kafka
        clean_env
        exit 0
        ;;
    "uninstall")
        uninstall_kafka
        exit 0
        ;;
esac
