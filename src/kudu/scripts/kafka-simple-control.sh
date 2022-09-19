#!/bin/bash

# a simple control for kafka standalone. for kudu duplicator itest

set -e

#TODO(duyuqi)
# Remove it, this is only for debug.
set -x

root=$(cd $(dirname $0); pwd)

usage() {
    echo "usage: sh $0 install|uninstall|start|stop|alive"
}

if [[ $# -lt 2 ]]; then
    echo "you should input an action"
    usage
    exit 1
fi

action=$1
port_offset=$2
keytab_path=$3
if [[ ! -z $keytab_path ]]; then
    principal=$4
fi

scala_version=2.12
kafka_version=3.2.3
kafka_package_name=kafka_${scala_version}-${kafka_version}
kafka_package=$kafka_package_name.tgz

download_url="https://archive.apache.org/dist/kafka/$kafka_version/$kafka_package"

topic_name=kudu_profile_record_stream
kafka_target=itest-kafka-$[2000+port_offset]

zookeeper_base_port=2181
kafka_base_port=9092
zookeeper_port=$[zookeeper_base_port+port_offset]
kafka_port=$[kafka_base_port+port_offset]
brokers_list=localhost:$kafka_port
zookeeper_list=localhost:$zookeeper_port
default_realm=""
kafka_service_name="kafka"

download_kafka() {
    pushd /tmp
    if [ x"$kafka_package" != x ] && [ -f $kafka_package ]; then
        echo "/tmp/$kafka_package has exist, skip download"
    else
        curl -LJO $download_url -o $kafka_package
    fi
    if [ x"$kafka_target" != x ] && [ -d $kafka_target ]; then
        uninstall_kafka
    fi
    
    mkdir -p tmp.$port_offset && cp -r $kafka_package tmp.$port_offset
    pushd tmp.$port_offset && tar xzf $kafka_package
    mv $kafka_package_name ../$kafka_target
    popd
    rm -rf tmp.$port_offset
    popd
}

prepare_zookeeper() {
    download_kafka
    pushd /tmp/$kafka_target
    # Prepare the ZooKeeper service configure items
    # Note: Soon, ZooKeeper will no longer be required by Apache Kafka.
    set +e
    if grep "^clientPort=" config/zookeeper.properties &>/dev/null; then
        sed -i "s/^clientPort=.*/clientPort=$zookeeper_port/g" config/zookeeper.properties
    else
        echo "clientPort=$zookeeper_port" >> config/zookeeper.properties
    fi
    # maxSessionTimeout
    if grep "^maxSessionTimeout=" config/zookeeper.properties &>/dev/null; then
        sed -i "s/^maxSessionTimeout=.*/maxSessionTimeout=10000/g" config/zookeeper.properties
    else
        echo "maxSessionTimeout=10000" >> config/zookeeper.properties
    fi
    mkdir -p /tmp/$kafka_target/zookeeper_data
    if grep "^dataDir=" config/zookeeper.properties &>/dev/null; then
        sed -i "s#^dataDir=.*#dataDir=/tmp/$kafka_target/zookeeper_data#g" config/zookeeper.properties
    else
        echo "dataDir=/tmp/$kafka_target/zookeeper_data" >> config/zookeeper.properties
    fi
    popd
}

prepare_kafka() {
    pushd /tmp/$kafka_target
    # Prepare the Kafka service configure items
    if grep "^listeners=PLAINTEXT://:" config/server.properties &>/dev/null; then
        sed -i "s#^listeners=PLAINTEXT://:.*#listeners=PLAINTEXT://:$kafka_port#g" config/server.properties
    else
        echo listeners=PLAINTEXT://:$kafka_port >> config/server.properties
    fi
    if grep "^advertised.listeners=PLAINTEXT://:" config/server.properties &>/dev/null; then
        sed -i "s#^advertised.listeners=PLAINTEXT://:.*#advertised.listeners=PLAINTEXT://:$kafka_port#g" config/server.properties
    else
        echo advertised.listeners=PLAINTEXT://:$kafka_port >> config/server.properties
    fi
    if grep "^zookeeper.connect=" config/server.properties &>/dev/null; then
        sed -i "s/^zookeeper.connect=.*/zookeeper.connect=$zookeeper_list/g" config/server.properties
    else
        echo zookeeper.connect=$zookeeper_list >> config/server.properties
    fi
    mkdir -p /tmp/$kafka_target/kafka_data
    if grep "^log.dirs=" config/server.properties &>/dev/null; then
        sed -i "s#^log.dirs=.*#log.dirs=/tmp/$kafka_target/kafka_data#g" config/server.properties
    else
        echo "log.dirs=/tmp/$kafka_target/kafka_data" >> config/server.properties
    fi
    if [[ ! -z $keytab_path ]]; then
        # install_kerberos
        sed -i "s#^listeners=PLAINTEXT://:.*#listeners=SASL_PLAINTEXT://localhost:$kafka_port#g" config/server.properties
        sed -i "s#^advertised.listeners=PLAINTEXT://:.*#advertised.listeners=SASL_PLAINTEXT://localhost:$kafka_port#g" config/server.properties
        echo "kerberos.auth.enable=true" >> config/server.properties
        echo "security.inter.broker.protocol=SASL_PLAINTEXT" >> config/server.properties
        echo "sasl.enabled.mechanisms=GSSAPI" >> config/server.properties
        echo "sasl.kerberos.service.name=$kafka_service_name" >> config/server.properties
        echo "sasl.kerberos.principal=$principal"
        echo "sasl.kerberos.keytab=$keytab_path"

        cat > config/kafka_server_jaas.conf <<EOF

KafkaServer {
  com.sun.security.auth.module.Krb5LoginModule required
  doNotPrompt=true
  useKeyTab=true
  storeKey=true
  keyTab="$keytab_path"
  principal="$principal";
};

EOF

        cat > config/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=$kafka_service_name
EOF

cat > config/kafka_client_jaas.conf <<EOF
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab="$keytab_path"
  principal="$principal";
};
EOF

        kdc_config_path=$(dirname $keytab_path);
        default_realm=$(grep default_realm $kdc_config_path/krb5.conf | awk '{print $NF}');
    fi
    set -e
    popd
}

start_zookeeper() {
    pushd /tmp/$kafka_target
    set -e
    # Start the Zookeeper service
    bin/zookeeper-server-start.sh config/zookeeper.properties \
        &> /tmp/$kafka_target/zookeeper.out </dev/null &
    sleep 2
    popd
}

start_kafka() {
    pushd /tmp/$kafka_target
    set -e

    # Start the Kafka broker service
    if [[ ! -z $keytab_path ]]; then
        export KAFKA_OPTS="-Dzookeeper.sasl.client=false -Djava.security.auth.login.config=config/kafka_server_jaas.conf -Djava.security.krb5.conf=$kdc_config_path/krb5.conf -Dsun.security.krb5.debug=true";
    fi
    bin/kafka-server-start.sh config/server.properties \
        &> /tmp/$kafka_target/kafka-server.out </dev/null &

    sleep 3
    popd
}

create_topic() {
    pushd /tmp/$kafka_target
    if [[ ! -z $keytab_path ]]; then
        export KAFKA_OPTS="-Djava.security.auth.login.config=config/kafka_client_jaas.conf -Djava.security.krb5.conf=$kdc_config_path/krb5.conf -Dsun.security.krb5.debug=true";
        echo "KAFKA_OPTS=$KAFKA_OPTS; bin/kafka-topics.sh --create --topic $topic_name --bootstrap-server $brokers_list --command-config config/client.properties"
        bin/kafka-topics.sh --create --topic $topic_name --bootstrap-server $brokers_list --command-config config/client.properties
        sleep 1
        bin/kafka-topics.sh --describe --topic $topic_name --bootstrap-server $brokers_list --command-config config/client.properties
    else
        bin/kafka-topics.sh --create --topic $topic_name --bootstrap-server $brokers_list
        sleep 1
        bin/kafka-topics.sh --describe --topic $topic_name --bootstrap-server $brokers_list
    fi
    
    echo "create_topic success"
    popd
}

delete_topic() {
    if [ ! -d /tmp/$kafka_target ]; then
        return
    fi
    pushd /tmp/$kafka_target
    set +e
    if [[ ! -z $keytab_path ]]; then
        export KAFKA_OPTS="-Djava.security.auth.login.config=config/kafka_client_jaas.conf -Djava.security.krb5.conf=$kdc_config_path/krb5.conf -Dsun.security.krb5.debug=true";
        bin/kafka-topics.sh --delete --topic $topic_name --bootstrap-server $brokers_list --command-config config/client.properties
        sleep 1
        bin/kafka-topics.sh --describe --topic $topic_name --bootstrap-server $brokers_list --command-config config/client.properties
    else
        bin/kafka-topics.sh --delete --topic $topic_name --bootstrap-server $brokers_list
        sleep 1
        bin/kafka-topics.sh --describe --topic $topic_name --bootstrap-server $brokers_list
    fi

    ret=$?
    set -e
    if [ $ret != 0 ]; then
        echo "delete_topic success"
    fi
    popd
}

test_write_and_read() {
    # Test kafka service ok? mannual confirm service

    pushd /tmp/$kafka_target
    # producer
    if [[ ! -z $keytab_path ]]; then
        bin/kafka-console-producer.sh --topic $topic_name --bootstrap-server $brokers_list --producer.config config/jaas.conf
    else
        bin/kafka-console-producer.sh --topic $topic_name --bootstrap-server $brokers_list
    fi

    # consumer
    if [[ ! -z $keytab_path ]]; then
        bin/kafka-console-consumer.sh --topic $topic_name --from-beginning --bootstrap-server $brokers_list --consumer.config config/jaas.conf
    else
        bin/kafka-console-consumer.sh --topic $topic_name --from-beginning --bootstrap-server $brokers_list
    fi
    popd
}

stop_zookeeper() {
    ## TODO(duyuqi) should fix it.
    # stop server.
    ps auxwf | grep $kafka_target | grep org.apache.zookeeper.server.quorum.QuorumPeerMain \
        | grep -v grep | awk '{print $2}' | xargs -i kill -9 {}
}

stop_kafka() {
    ## TODO(duyuqi) should fix it.
    # stop server.
    ps auxwf | grep $kafka_target | grep kafka.Kafka | grep -v grep \
        | awk '{print $2}' | xargs -i kill -9 {}
}

kafka_alive() {
    set +e
    set -o pipefail
    sleep 2
    ps auxwf | grep $kafka_target | grep kafka.Kafka | grep -v grep
    ret=$?
    set -e
    set +o pipefail
    return $ret
}

uninstall_kafka() {
    stop_kafka
    clean_env
}

clean_env() {
    rm -rf /tmp/$kafka_target /tmp/$kafka_target/zookeeper_data /tmp/kafka-logs
}

case $action in 
    "install")
        prepare_zookeeper
        prepare_kafka
        start_zookeeper
        start_kafka
        create_topic
        exit 0
        ;;
    "uninstall")
        delete_topic
        stop_kafka
        stop_zookeeper
        clean_env
        exit 0
        ;;
    "start")
        start_kafka
        exit 0
        ;;
    "alive")
        kafka_alive
        exit $?
        ;;
    "stop")
        stop_kafka
        exit 0
        ;;
esac
