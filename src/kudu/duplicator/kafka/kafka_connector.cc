// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// the codes based on some ningw's internal work, and then refact.
//

#include "kudu/duplicator/kafka/kafka_connector.h"

#include <chrono>
#include <ostream>
#include <string>
#include <utility>

#include <cppkafka/configuration.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/message_builder.h>
#include <cppkafka/metadata.h>
#include <cppkafka/producer.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/monotime.h"

using std::string;
using std::vector;
using strings::Substitute;

// support kerberos, detail in
// https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka, the page's the 5th
// part: 5.Configure Kafka client on client host
DEFINE_string(kafka_connector_security_protocol, "SASL_PLAINTEXT",
              "security protocol, such as PLAINTEXT,SSL,SASL_PLAINTEXT,SASL_SSL");
DEFINE_string(kafka_connector_keytab_full_path, "", "keytab file's full path");
DEFINE_string(kafka_connector_principal_name, "", "principal info, your should input correct info");

static bool ValidatorKafkaConnectorGroupFlags() {
  if (FLAGS_kafka_connector_keytab_full_path.empty() !=
      FLAGS_kafka_connector_principal_name.empty()) {
    LOG(ERROR) << "kafka_connector_keytab_full_path and kafka_connector_principal_name flags should"
                  "set together or default together";
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(kafka_connector_group_flags, ValidatorKafkaConnectorGroupFlags);

DEFINE_int32(kafka_connector_min_topic_partition_num, 1, "the min value of topic's partition num");
DEFINE_int32(kafka_connector_flush_timeout_ms, 10000, "timeout of producer write kafka");
TAG_FLAG(kafka_connector_flush_timeout_ms, runtime);

METRIC_DEFINE_entity(kafka_connector);

METRIC_DEFINE_counter(kafka_connector,
                      duplication_ops,
                      "Duplication Ops",
                      kudu::MetricUnit::kRequests,
                      "Number of ops that write to kafka while LEADER from bootstrap.",
                      kudu::MetricLevel::kInfo);

METRIC_DEFINE_histogram(kafka_connector,
                        kafka_flush_duration,
                        "Kafka Flush Duration",
                        kudu::MetricUnit::kMilliseconds,
                        "Time spent flushing Kafka.",
                        kudu::MetricLevel::kInfo,
                        60000LU,
                        1);

namespace kudu {
namespace kafka {

KafkaConnector::KafkaConnector(duplicator::ConnectorOptions options)
    : options_(std::move(options)), brokers_(options_.uri) {
  metric_entity_ = METRIC_ENTITY_kafka_connector.Instantiate(&metric_registry_, "kafka_connector");
  duplication_ops_ = metric_entity_->FindOrCreateCounter(&METRIC_duplication_ops);
  kafka_flush_histogram_ = METRIC_kafka_flush_duration.Instantiate(metric_entity_);
}

KafkaConnector::~KafkaConnector() {
  try {
    if (producer_) {
      producer_->flush(std::chrono::milliseconds(FLAGS_kafka_connector_flush_timeout_ms));
      producer_.reset();
    }
  } catch (const cppkafka::Exception& e) {
    LOG(WARNING) << "flush failed when destruct: " << e.what();
  }
}

Status KafkaConnector::Init(const duplicator::ConnectorOptions& options) {
  cppkafka::Configuration configuration = {{"metadata.broker.list", brokers_}};

  if (!FLAGS_kafka_connector_keytab_full_path.empty()) {
    configuration.set("security.protocol", FLAGS_kafka_connector_security_protocol);
    configuration.set("sasl.kerberos.keytab", FLAGS_kafka_connector_keytab_full_path);
    configuration.set("sasl.kerberos.principal", FLAGS_kafka_connector_principal_name);
  }

  auto producer = std::make_shared<cppkafka::Producer>(configuration);
  producer_.swap(producer);

  string topic_name = options.name;
  try {
    cppkafka::Metadata metadata = producer_->get_metadata();
    bool found = false;
    // The topic should have been created at program running envs
    bool partition_num_valid = false;
    for (const auto& topic : metadata.get_topics()) {
      if (topic.get_name() == topic_name) {
        found = true;
        if (topic.get_partitions().size() >= FLAGS_kafka_connector_min_topic_partition_num) {
          partition_num_valid = true;
        }
        break;
      }
    }
    if (!found) {
      // topic not found
      RETURN_NOT_OK_LOG(
          Status::Aborted(Substitute("topic $0 not exist", topic_name)), ERROR, "need check topic");
    }
    if (!partition_num_valid) {
      RETURN_NOT_OK_LOG(Status::Aborted(Substitute("topic $0 partition num should more than $1",
                                                   topic_name,
                                                   FLAGS_kafka_connector_min_topic_partition_num)),
                        ERROR,
                        "need check partition num");
    }
  } catch (const cppkafka::Exception& e) {
    LOG(ERROR) << "check whether topic: " << topic_name << " exists";
    LOG(ERROR) << "check whether broker list " << brokers_ << " is right";
    return Status::Corruption(
        Substitute("check whether topic $0 exists or broker $1 works fine", topic_name, brokers_));
  }

  LOG(INFO) << "kafka producer init succeed!";
  return Status::OK();
}

// consume content in kafka_record_queue and flush to kafka
Status KafkaConnector::WriteBatch(
    const string& topic_name,
    const vector<std::unique_ptr<duplicator::DuplicateMsg>>& messages) {
  CHECK(producer_);
  if (messages.empty()) {
    return Status::OK();
  }
  // TODO(duyuqi)
  // Limit the number of kafka_messages
  for (const auto& duplicate_message : messages) {
    for (const auto& kafka_message : duplicate_message->result()) {
      string json_data;
      string& hash_key_str = kafka_message->primary_key;
      kudu::kafka::RawKuduRecord& record = kafka_message->record;
      record.SerializeToString(&json_data);
      try {
        producer_->produce(
            cppkafka::MessageBuilder(topic_name).key(hash_key_str).payload(json_data));
      } catch (const cppkafka::Exception& e) {
        LOG(WARNING) << "produce message failed, try to re new producer, " << e.what();
        return Status::ServiceUnavailable("kafka client produce failed");
      }
    }
  }

  try {
    MonoTime before = MonoTime::Now();
    producer_->flush(std::chrono::milliseconds(FLAGS_kafka_connector_flush_timeout_ms));
    MonoDelta elapsed = MonoTime::Now() - before;
    duplication_ops_->IncrementBy(messages.size());
    kafka_flush_histogram_->Increment(elapsed.ToMicroseconds());
    return Status::OK();
  } catch (const cppkafka::Exception& e) {
    LOG(WARNING) << "failed when flushing: " << e.what();
  }

  return Status::ServiceUnavailable("flush messages failed");
}

Status KafkaConnector::TestBasicClientApi(const string& topic_name, const string& msg) {
  try {
    producer_->produce(cppkafka::MessageBuilder(topic_name).key("hello world").payload(msg));
  } catch (const cppkafka::Exception& e) {
    return Status::IOError("basic produce error");
  }
  try {
    producer_->flush(std::chrono::milliseconds(FLAGS_kafka_connector_flush_timeout_ms));
  } catch (const cppkafka::Exception& e) {
    return Status::IOError("basic flush error");
  }

  return Status::OK();
}

}  // namespace kafka
}  // namespace kudu
