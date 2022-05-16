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

#include "kudu/duplicator/kafka/kafka_connector.h"

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/metadata.h>
#include <cppkafka/producer.h>
#include <cppkafka/topic_partition_list.h>
#include <glog/logging.h>
#include <google/protobuf/arena.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/duplicator/connector.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/tablet/row_op.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
class RowChangeList;

namespace tablet {
class WriteOpState;
}  // namespace tablet
}  // namespace kudu

using std::make_shared;

namespace kudu {
namespace duplicator {

class KafkaConnectorTest : public KuduTest {
 public:
  KafkaConnectorTest()
      : schema_(GetSimpleSchema()),
        client_schema_(schema_.CopyWithoutColumnIds()),
        arena_(256 * 1024) {
    connector_ = kafka::KafkaConnector::GetInstance();
  }
  void SetUp() override {
    DestroyKafka();
    InitKafka();
  }
  void TearDown() override {
    DestroyKafka();
  }

  static Schema GetSimpleSchema() {
    SchemaBuilder builder;
    builder.AddKeyColumn("k_int8", INT8);
    builder.AddKeyColumn("k_int16", INT16);
    builder.AddKeyColumn("k_int32", INT32);
    builder.AddKeyColumn("k_int64", INT64);
    builder.AddKeyColumn("k_string", STRING);
    builder.AddKeyColumn("k_binary", BINARY);
    builder.AddKeyColumn("k_timestamp", UNIXTIME_MICROS);

    // nullable default: false
    builder.AddColumn("v_int8", INT8, false, nullptr, nullptr);
    builder.AddColumn("v_int16", INT16, false, nullptr, nullptr);
    builder.AddColumn("v_int32", INT32, false, nullptr, nullptr);
    builder.AddColumn("v_int64", INT64, false, nullptr, nullptr);
    builder.AddColumn("v_string", STRING, false, nullptr, nullptr);
    builder.AddColumn("v_binary", BINARY, false, nullptr, nullptr);
    builder.AddColumn("v_timestamp", UNIXTIME_MICROS, false, nullptr, nullptr);

    int32_t default_read = -1;
    int16_t default_write = 0;
    kudu::Slice default_read_str("read_empty");
    kudu::Slice default_write_str("write_empty");
    builder.AddColumn("vn_int8", INT8, true, nullptr, nullptr);
    builder.AddColumn("vn_int16", INT16, true, nullptr, &default_write);
    builder.AddColumn("vn_int32", INT32, true, &default_read, nullptr);
    builder.AddColumn("vn_int64", INT64, true, nullptr, nullptr);
    builder.AddColumn("vn_string", STRING, true, &default_read_str, nullptr);
    builder.AddColumn("vn_binary", BINARY, true, nullptr, &default_write_str);
    builder.AddColumn("vn_timestamp", UNIXTIME_MICROS, true, nullptr, nullptr);

    return builder.Build();
  }

  Status GenDecodedRowOperation(std::vector<DecodedRowOperation>* ops) {
    int8_t int8_expected = 0xF0;
    int16_t int16_expected = 0xFFF0;
    int32_t int32_expected = 0xFFFFF0;
    int64_t int64_expected = 0xFFFFFFFF0;

    kudu::RowOperationsPB pb;
    RowOperationsPBEncoder encoder(&pb);

    int count = 0;
    RowOperationsPB::Type type_list[] = {RowOperationsPB::INSERT,
                                         RowOperationsPB::UPDATE,
                                         RowOperationsPB::DELETE,
                                         RowOperationsPB::UPSERT,
                                         RowOperationsPB::INSERT_IGNORE,
                                         RowOperationsPB::UPDATE_IGNORE,
                                         RowOperationsPB::DELETE_IGNORE};
    for (RowOperationsPB::Type type : type_list) {
      count++;
      kudu::KuduPartialRow* row_ptr = new kudu::KuduPartialRow(&client_schema_);
      kudu::KuduPartialRow& row = *row_ptr;
      CHECK_OK(row.SetInt8("k_int8", int8_expected + static_cast<int8_t>(count)));
      CHECK_OK(row.SetInt16("k_int16", int16_expected + static_cast<int16_t>(count)));
      CHECK_OK(row.SetInt32("k_int32", int32_expected + static_cast<int32_t>(count)));
      CHECK_OK(row.SetInt64("k_int64", int64_expected + static_cast<int64_t>(count)));
      CHECK_OK(row.SetStringNoCopy("k_string", "string-value"));
      CHECK_OK(row.SetBinaryNoCopy("k_binary", "binary-value"));
      CHECK_OK(row.SetUnixTimeMicros("k_timestamp", 10009));

      CHECK_OK(row.SetInt8("v_int8", int8_expected));
      CHECK_OK(row.SetInt16("v_int16", int16_expected));
      CHECK_OK(row.SetInt32("v_int32", int32_expected));
      CHECK_OK(row.SetInt64("v_int64", int64_expected));
      CHECK_OK(row.SetStringNoCopy("v_string", "string-value"));
      CHECK_OK(row.SetBinaryNoCopy("v_binary", "binary-value"));
      CHECK_OK(row.SetUnixTimeMicros("v_timestamp", 10009));

      CHECK_OK(row.SetInt8("vn_int8", int8_expected + static_cast<int32_t>(count)));
      CHECK_OK(row.SetInt64("vn_int64", int64_expected + static_cast<int64_t>(count)));
      CHECK_OK(row.SetBinaryNoCopy("vn_binary", "binary-value"));
      encoder.Add(type, row);
    }
    RowOperationsPBDecoder decoder(&pb, &client_schema_, &schema_, &arena_);
    decoder.DecodeOperations<DecoderMode::WRITE_OPS>(ops);
    return Status::OK();
  }
  static Status GenRowChangeList(RowChangeList* /* change_list */) { return Status::OK(); }

private:
  void InitKafka() {
    std::string start_cmd = "sh /tmp/kafka-simple-control.sh start";
    int ret = system(start_cmd.c_str());
    if (ret != 0) {
      LOG(FATAL) << "start kafka failed";
    }

    cppkafka::Configuration configuration = {
      { "metadata.broker.list", "localhost:9092" },
      { "group.id", "default" },
      { "enable.auto.commit", false }
    };

    consumer_ = std::make_shared<cppkafka::Consumer>(configuration);

    // Print the assigned partitions on assignment
    consumer_->set_assignment_callback([](const cppkafka::TopicPartitionList& partitions) {
      LOG(INFO) << "Got assigned: " << partitions;
    });

    // Print the revoked partitions on revocation
    consumer_->set_revocation_callback([](const cppkafka::TopicPartitionList& partitions) {
      LOG(INFO)  << "Got revoked: " << partitions;
    });

    consumer_->subscribe({ "kudu_profile_record_stream" });
  }

  static void DestroyKafka() {
    std::string stop_cmd = "sh /tmp/kafka-simple-control.sh stop";
    int ret = system(stop_cmd.c_str());
    if (ret != 0) {
      LOG(FATAL) << "stop kafka failed";
    }
  }

public:
  Schema schema_;
  Schema client_schema_;
  Arena arena_;
  Connector* connector_;
  std::shared_ptr<cppkafka::Consumer> consumer_;
};

using kudu::kafka::RawKuduRecord;

TEST_F(KafkaConnectorTest, ParseRow) {
  std::vector<DecodedRowOperation> ops;
  GenDecodedRowOperation(&ops);
  ASSERT_EQ(7, ops.size());

  RawKuduRecord record1;
  uint64_t code1 = 0;
  ASSERT_TRUE(kafka::ParseRow(ops[0], &schema_, &record1, &code1).ok());

  for (int i = 1; i < 7; i++) {
    RawKuduRecord record2;
    uint64_t code2 = 0;
    uint64_t code3 = 0;
    ASSERT_TRUE(kafka::ParseRow(ops[i], &schema_, &record2, &code2).ok());
    kudu::SleepFor(kudu::MonoDelta::FromMilliseconds(100));
    ASSERT_TRUE(kafka::ParseRow(ops[i], &schema_, &record2, &code3).ok());
    ASSERT_EQ(code2, code3);
  }
}

TEST_F(KafkaConnectorTest, KafkaProducerWritebatchTest) {
  arena_.Reset();
  connector_->InitPrepare();

  std::vector<DecodedRowOperation> ops;
  GenDecodedRowOperation(&ops);
  std::vector<tablet::RowOp*> row_ops;
  row_ops.reserve(ops.size());
  for (auto& op : ops) {
    google::protobuf::Arena pb_arena;
    row_ops.emplace_back(arena_.NewObject<tablet::RowOp>(&pb_arena, std::move(op)));
  }

  std::shared_ptr<Schema> schema_ptr(new Schema(schema_));
  std::string table_name("bailing");

  std::vector<std::shared_ptr<duplicator::DuplicateMsg>> messages;
  std::shared_ptr<tablet::WriteOpState> empty_ptr;
  for (auto& row_op : row_ops) {
    std::shared_ptr<duplicator::DuplicateMsg> dup_msg =
      std::make_shared<duplicator::DuplicateMsg>(empty_ptr, row_op, schema_ptr, table_name);
    messages.emplace_back(dup_msg);
  }

  LOG(INFO) << "kafka connector running, write to kafka";
  ASSERT_OK(connector_->Init());
  ASSERT_OK(connector_->TestBasicClientApi("write kafka basic test"));
  ASSERT_OK(connector_->WriteBatch(messages));
}


}  // namespace duplicator
}  // namespace kudu
