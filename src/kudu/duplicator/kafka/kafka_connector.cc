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
#include <functional>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include <cppkafka/buffer.h>
#include <cppkafka/configuration.h>
#include <cppkafka/error.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/kafka_handle_base.h>
#include <cppkafka/message.h>
#include <cppkafka/message_builder.h>
#include <cppkafka/metadata.h>
#include <cppkafka/producer.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <librdkafka/rdkafka.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"

using std::string;
using std::vector;
using strings::Substitute;

DEFINE_int32(kafka_connector_producer_queue_max_size, 100000,
            "queue.buffering.max.messages of librdkafka. at librdkafka default 100000");
DEFINE_int32(kafka_connector_producer_queue_max_kbytes, 1048576,
             "queue.buffering.max.kbyte of librdkafka. at librdkafka default 0x1000000(1GB)");
DEFINE_bool(enable_wait_producer_flush_for_testing, false, "Just for test");

DEFINE_int32(kafka_connector_min_topic_partition_num, 1, "the min value of topic's partition num");
DEFINE_int32(kafka_connector_flush_timeout_ms, 20000, "timeout of producer write kafka");
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
      do {
        producer_->flush(std::chrono::milliseconds(FLAGS_kafka_connector_flush_timeout_ms));
      } while (FLAGS_enable_wait_producer_flush_for_testing &&
               producer_->get_out_queue_length() > 0);
      producer_.reset();
    }
  } catch (const cppkafka::Exception& e) {
    LOG(WARNING) << Substitute("flush failed when destruct: $0", e.what());
  }
}

Status KafkaConnector::Init(const duplicator::ConnectorOptions& options) {
  cppkafka::Configuration configuration = {
      {"metadata.broker.list", brokers_},
      {"queue.buffering.max.messages", FLAGS_kafka_connector_producer_queue_max_size},
      {"queue.buffering.max.kbytes", FLAGS_kafka_connector_producer_queue_max_kbytes}};

  if (options.kerberos_options) {
    if (options.kerberos_options->security_protocol() ==
            kudu::KerberosOptions::UNKNOWN_SECURITY_PROTOCOL ||
        options.kerberos_options->keytab().empty() !=
            options.kerberos_options->principal().empty()) {
      string message =
          Substitute("validate kerberos options failed, options: $0", options.ToString());
      RETURN_NOT_OK_LOG(Status::Corruption(message), ERROR, message);
    }
    // support kerberos, detail in
    // https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka, the page's the 5th
    // part: 5.Configure Kafka client on client host
    configuration.set("security.protocol",
                      kudu::KerberosOptions::SecurityProtocol_Name(
                          options.kerberos_options->security_protocol()));
    configuration.set("sasl.kerberos.service.name", options.kerberos_options->service_name());
    configuration.set("sasl.kerberos.keytab", options.kerberos_options->keytab());
    configuration.set("sasl.kerberos.principal", options.kerberos_options->principal());
  }

  configuration.set_delivery_report_callback(std::bind(
      &KafkaConnector::DeliveryReport, this, std::placeholders::_1, std::placeholders::_2));

  configuration.set_error_callback(
      [](cppkafka::KafkaHandleBase& handle, int error, const std::string& reason) {
        cppkafka::Producer& producer = down_cast<cppkafka::Producer&>(handle);
        LOG(ERROR) << Substitute(
            "Error callback, out_queue_length: $0, error_code: $1, error_msg: $2, reason: $3",
            producer.get_out_queue_length(), error,
            ::rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(error)), reason);
      });

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
    string message = Substitute(
        "check whether options for kafka are right: $0, error: $1", options.ToString(), e.what());
    RETURN_NOT_OK_LOG(Status::Corruption(message), ERROR, message);
  }

  LOG(INFO) << "kafka producer init succeed!";
  return Status::OK();
}

// consume content in kafka_record_queue and flush to kafka
Status KafkaConnector::WriteBatch(
    const string& topic_name,
    const vector<std::unique_ptr<duplicator::DuplicateMsg>>& messages) {
  CHECK(producer_);
  if (PREDICT_FALSE(messages.empty())) {
    if (producer_->get_out_queue_length() > 0) {
      return Flush();
    }
    return Status::OK();
  }
  std::lock_guard<Mutex> l(mutex_);
  // TODO(duyuqi)
  // Limit the number of kafka_messages(flow control).
  for (const auto& duplicate_message : messages) {
    for (const auto& kafka_message : duplicate_message->result()) {
      string json_data;
      const string& primary_key_str = kafka_message->primary_key;
      const kudu::kafka::RawKuduRecord& record = kafka_message->record;
      record.SerializeToString(&json_data);

      int retry = 3;
      do {
        try {
          producer_->produce(
              cppkafka::MessageBuilder(topic_name).key(primary_key_str).payload(json_data));
          break;
        } catch (const cppkafka::HandleException& e) {
          if (e.get_error().get_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            LOG(WARNING) << Substitute("produce message queue full, would retry flush it, $0",
                                       e.what());
            if (Flush().ok() && retry-- > 0) {
              continue;
            }
          }
          LOG(WARNING) << Substitute("produce message failed, user would retry it, key: $0, $1",
                                     primary_key_str, e.what());
          return Status::Incomplete("kafka client produce failed");
        }
      } while (true);
    }
  }

  return Flush();
}

Status KafkaConnector::Flush() {
  try {
    MonoTime before = MonoTime::Now();
    producer_->flush(std::chrono::milliseconds(FLAGS_kafka_connector_flush_timeout_ms));
    MonoDelta elapsed = MonoTime::Now() - before;
    kafka_flush_histogram_->Increment(elapsed.ToMicroseconds());
    return Status::OK();
  } catch (const cppkafka::HandleException& e) {
    LOG(WARNING) << Substitute("failed when flushing: $0", e.what());
  }
  return Status::RemoteError("flush messages failed");
}

Status KafkaConnector::TestBasicClientApi(const string& topic_name, const string& msg) {
  std::unique_lock<Mutex> l(mutex_);
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

void KafkaConnector::DeliveryReport(cppkafka::Producer& /*producer*/,
                                    const cppkafka::Message& message) {
  string key = string(reinterpret_cast<const char*>(message.get_key().get_data()),
                      message.get_key().get_size());
  if (message.get_error().get_error() != RD_KAFKA_RESP_ERR_NO_ERROR) {
    LOG(WARNING) << Substitute("DeliveryReport fail, would retry it, key: $0, error: $1",
                               key, message.get_error().to_string());
    // TODO(duyuqi)
    // The error can be solved by patch librdkafka, using the method to solve the problem.
    // The simple method may cause out of order in ops.
    // Solve it later.
    do {
      try {
        producer_->produce(message);
        break;
      } catch (const cppkafka::HandleException& e) {
        LOG(INFO) << Substitute(
            "retry produce because of DeliveryReport fail, key: $0, error: $1", key, e.what());
        SleepFor(MonoDelta::FromMilliseconds(200));
        continue;
      }
    } while (true);
    return;
  }
  // TODO(duyuqi)
  // The metric is count of records written to kafka, if errors happen,
  // redundancy records would send to kafka.
  // Whether adding another metric for the duplication success count?
  duplication_ops_->Increment();
}

}  // namespace kafka
}  // namespace kudu
