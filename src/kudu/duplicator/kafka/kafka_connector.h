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

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <cppkafka/configuration.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/message_builder.h>
#include <cppkafka/metadata.h>
#include <cppkafka/producer.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/duplicator/connector.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/singleton.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/row_op.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {
class RowChangeList;
}  // namespace kudu

DECLARE_string(kafka_broker_list);
DECLARE_string(kafka_target_topic);
DECLARE_int32(kafka_retry_num);
DECLARE_string(downstream_type);

DECLARE_string(security_protocol);
DECLARE_string(keytab_full_path);
DECLARE_string(principal_name);
DECLARE_int32(min_topic_partition_num);

namespace kudu {
namespace kafka {

extern Status ParseRow(const DecodedRowOperation& decoded_row,
                    const Schema* schema,
                    RawKuduRecord* insert_value_pb,
                    uint64_t* primary_key_hash_code);
extern Status ChangesToColumn(const RowChangeList& changelist,
                           const Schema* schema,
                           RawKuduRecord* update_value_pb);
template <class RowType>
void RangeToPB(const Schema& schema,
               const RowType& row,
               const uint8_t* isset_bitmap,
               kudu::kafka::RawKuduRecord* insert_value_pb) {
  int low = schema.num_key_columns();
  int high = schema.num_columns();
  for (int i = low; i < high; ++i) {
    if (!BitmapTest(isset_bitmap, i)) {
      continue;
    }
    const ColumnSchema& col = schema.column(i);
    auto* property = insert_value_pb->add_properties();
    if (col.is_nullable() && row.cell(i).is_null()) {
      property->set_value("");
      property->set_null_value(true);
    } else {
      property->set_null_value(false);
      std::string ret;
      col.type_info()->StringForValue(row.cell(i).ptr(), &ret);
      property->set_value(ret);
    }
    property->set_name(col.name());
    property->set_primary_key_column(false);
  }
}

template <class RowType>
void RangePKToPB(const Schema& schema,
                 const RowType& row,
                 kudu::kafka::RawKuduRecord* insert_value_pb,
                 uint64_t* primary_key_hash_code) {
  int pk_num = schema.num_key_columns();
  std::string primary_key;
  for (int i = 0; i < pk_num; ++i) {
    const ColumnSchema& col = schema.column(i);
    std::string ret;
    col.type_info()->StringForValue(row.cell(i).ptr(), &ret);
    auto* property = insert_value_pb->add_properties();
    property->set_name(col.name());
    property->set_value(ret);
    property->set_primary_key_column(true);
    property->set_null_value(false);
    primary_key.append(ret);
  }
  *primary_key_hash_code = util_hash::CityHash64(primary_key.data(), primary_key.size());
}

// template <typename T = cppkafka::Producer>
class KafkaConnector : public duplicator::RemoteConnector {
 public:
  static KafkaConnector* GetInstance() {
    if (FLAGS_downstream_type != "kafka") {
      return nullptr;
    }
    return Singleton<KafkaConnector>::get();
  }

  Status InitPrepare() override {
    cppkafka::Configuration configuration = {{"metadata.broker.list", FLAGS_kafka_broker_list}};

    if (FLAGS_keytab_full_path.empty() != FLAGS_principal_name.empty()) {
      LOG(FATAL) << "keytab_full_path and principal_name flags should"
          "set together or default together";
    }
    if (!FLAGS_keytab_full_path.empty()) {
      configuration.set("security.protocol", FLAGS_security_protocol);
      configuration.set("sasl.kerberos.keytab", FLAGS_keytab_full_path);
      configuration.set("sasl.kerberos.principal", FLAGS_principal_name);
    }

    auto producer = std::make_shared<cppkafka::Producer>(configuration);
    producer_.swap(producer);
    return Status::OK();
  }

  Status Init() override {
    if (inited_) {
      return Status::OK();
    }
    std::lock_guard<Mutex> l(init_lock_);
    if (inited_) {
      return Status::OK();
    }
    InitPrepare();
    try {
      cppkafka::Metadata metadata = producer_->get_metadata();
      bool found = false;
      // The topic should have been created at program running envs
      bool partition_num_valid = false;
      for (const auto& topic : metadata.get_topics()) {
        if (topic.get_name() == FLAGS_kafka_target_topic) {
          found = true;
          if (topic.get_partitions().size() >= FLAGS_min_topic_partition_num) {
            partition_num_valid = true;
          }
          break;
        }
      }
      if (!found) {
        // topic not found
        std::string s = strings::Substitute("topic $0 not exist", FLAGS_kafka_target_topic);
        LOG(ERROR) << s;
        return Status::Aborted(s);
      }
      if (!partition_num_valid) {
        std::string s = strings::Substitute("topic $0 partition num should more than $1",
                                            FLAGS_kafka_target_topic,
                                            FLAGS_min_topic_partition_num);
        LOG(ERROR) << s;
        return Status::Aborted(s);
      }
    } catch (const cppkafka::Exception& e) {
      LOG(ERROR) << "check whether topic: " << FLAGS_kafka_target_topic << " exists";
      LOG(ERROR) << "check whether broker list " << FLAGS_kafka_broker_list << " is right";
      return Status::Corruption(
          strings::Substitute("check whether topic $0 exists or broker $1 works fine",
                              FLAGS_kafka_target_topic,
                              FLAGS_kafka_broker_list));
    }

    LOG(INFO) << "kafka producer init succeed!";
    inited_ = true;
    return Status::OK();
  }

  // consume content in kafka_record_queue and flush to kafka
  Status WriteBatch(
      const std::vector<std::shared_ptr<duplicator::DuplicateMsg>>& messages) override {
    if (messages.empty()) {
      return Status::OK();
    }
    for (const auto& entry : messages) {
      kudu::kafka::RawKuduRecord record;
      uint64_t primary_key_hash_code = 0;
      RETURN_NOT_OK(ParseRow(entry->row_op->decoded_op, entry->schema_ptr.get(),
                             &record, &primary_key_hash_code));
      record.set_table_name(std::move(entry->table_name));
      switch (entry->row_op->decoded_op.type) {
        case RowOperationsPB::INSERT:
        case RowOperationsPB::INSERT_IGNORE:
        case RowOperationsPB::UPSERT:
        case RowOperationsPB::UPDATE:
        case RowOperationsPB::UPDATE_IGNORE:
          record.set_operation_type(kudu::kafka::RawKuduRecord::SET);
          break;
        case RowOperationsPB::DELETE:
        case RowOperationsPB::DELETE_IGNORE:
          record.set_operation_type(kudu::kafka::RawKuduRecord::DELETE);
          break;
        default:
          LOG(WARNING) << "unknown RowOperationsPB type" <<
            RowOperationsPB::Type_Name(entry->row_op->decoded_op.type);
          break;
      }

      std::string hash_key(reinterpret_cast<const char*>(&primary_key_hash_code), sizeof(uint64_t));
      std::string json_data;
      record.SerializeToString(&json_data);
      const auto& message = cppkafka::MessageBuilder(FLAGS_kafka_target_topic)
                                .key(hash_key)
                                .payload(json_data);

      try {
        producer_->produce(message);
      } catch (const cppkafka::Exception& e) {
        LOG(WARNING) << "produce message failed";
        return Status::ServiceUnavailable("kafka client produce failed");
      }
    }

    try {
      producer_->flush(flush_timeout_ms_);
      return Status::OK();
    } catch (const cppkafka::Exception& e) {
      LOG(WARNING) << "failed when flushing: " << e.what();
    }

    return Status::ServiceUnavailable("flush messages failed");
  }

  Status TestBasicClientApi(const std::string& msg) override {
    const auto& message =
        cppkafka::MessageBuilder(FLAGS_kafka_target_topic)
            .key("hello world")
            .payload(msg);
    try {
      producer_->produce(message);
    } catch (const cppkafka::Exception& e) {
      return Status::IOError("basic produce error");
    }
    try {
      producer_->flush(flush_timeout_ms_);
    } catch (const cppkafka::Exception& e) {
      return Status::IOError("basic flush error");
    }

    return Status::OK();
  }

  std::shared_ptr<cppkafka::Producer> producer() {
    return producer_;
  }

 protected:
  KafkaConnector() : duplicator::RemoteConnector(), flush_timeout_ms_(10000) {}
  ~KafkaConnector() override {}
  friend class Singleton<KafkaConnector>;
  std::chrono::milliseconds flush_timeout_ms_;

 protected:
  // cppkafka::Producer, cppkafka::MockProducer
  std::shared_ptr<cppkafka::Producer> producer_;

  mutable Mutex init_lock_;
  bool inited_ = false;
};

}  // namespace kafka
}  // namespace kudu
