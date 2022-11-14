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

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <cppkafka/buffer.h>
#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cppkafka/error.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/message.h>
#include <cppkafka/topic_partition_list.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/single_broker_kafka.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int64(duplicator_max_queue_size);
DECLARE_int32(kafka_connector_flush_timeout_ms);

using std::string;
using std::vector;
using kudu::kafka::RawKuduRecord;

namespace kudu {

namespace rpc {
class Messenger;
}  // namespace rpc

struct CreateTableOptions {
  string table_name;
  int32_t partition_num;
  int32_t replication_refactor;
  std::optional<client::DuplicationInfo> dup_info;
};

enum class Action { kEnableDuplication = 0, kDisableDuplication };

struct AlterTableOptions {
  Action action;
  string table_name;
  client::DuplicationInfo info;
};

// Simple base utility class to provide an external mini cluster with common
// setup routines useful for integration tests. And start kafka service to test
// data duplication.
class DuplicationITest : public ExternalMiniClusterITestBase {
 public:
  // default topic.
  const string kTopicName = "kudu_profile_record_stream";
  // default kafka uri(brokers).
  const int kOffsetPort = 7;
  const string kBrokers =
      Substitute("localhost:$0", duplication::kafka::kKafkaBasePort + kOffsetPort);

  const string kSmallest = "smallest";
  const string kLargest = "largest";

  DuplicationITest()
      : kafka_(kOffsetPort), schema_(client::KuduSchema::FromSchema(GetSimpleTestSchema())) {
    cluster_opts_.num_tablet_servers = 4;

    // cluster_opts_.extra_master_flags.emplace_back("--v=64");
    // cluster_opts_.extra_tserver_flags.emplace_back("--v=64");
    cluster_opts_.extra_tserver_flags.emplace_back("--log_segment_size_mb=1");
  }
  void SetUp() override {
    NO_FATALS(StartClusterWithOpts(cluster_opts_));
    kafka_.DestroyKafka();
    kafka_.InitKafka();
  }
  void TearDown() override {
    ExternalMiniClusterITestBase::TearDown();
    if (consumer_) {
      consumer_->unsubscribe();
    }
    kafka_.DestroyKafka();
  }

  void WriteRows(int from, int to) { ASSERT_OK(InsertRows(from, to)); }

  void RenewConsumer(const cppkafka::Configuration& configuration) {
    consumer_.reset();
    consumer_ = std::make_shared<cppkafka::Consumer>(configuration);

    // Print the assigned partitions on assignment
    consumer_->set_assignment_callback([](const cppkafka::TopicPartitionList& partitions) {
      LOG(INFO) << "Got assigned: " << partitions;
    });
    // Print the revoked partitions on revocation
    consumer_->set_revocation_callback([](const cppkafka::TopicPartitionList& partitions) {
      LOG(INFO) << "Got revoked: " << partitions;
    });
    consumer_->subscribe({kTopicName});
  }

  void Consume(int expected_size, const string& group_name, const string& strategy) {
    cppkafka::Configuration configuration = {{"metadata.broker.list", kBrokers},
                                             {"group.id", group_name},
                                             {"enable.auto.commit", false},
                                             {"auto.offset.reset", strategy}};

    RenewConsumer(configuration);

    std::chrono::milliseconds timeout(2000);
    int count = 0;
    int max_batch_size = 16;
    vector<cppkafka::Message> messages;
    int empty_count = 0;
    std::set<int32_t> key_set;
    while (true) {
      messages.clear();
      try {
        messages = consumer_->poll_batch(max_batch_size, timeout);
      } catch (const cppkafka::Exception& e) {
        LOG(WARNING) << "consumer exception: " << e.what();
        // RenewConsumer(configuration);
        SleepFor(MonoDelta::FromMilliseconds(100));
        continue;
      }
      if (messages.empty()) {
        VLOG(0) << "kafka consumer poll messages empty, wait 2s";
        if (empty_count++ >= 15) {
          break;
        }
        continue;
      }
      empty_count = 0;
      for (const auto& msg : messages) {
        if (!msg) {
          continue;
        }
        if (msg.get_error() && !msg.is_eof()) {
          LOG(INFO) << "Received error notification: " << msg.get_error();
          continue;
        }

        if (!msg.get_error()) {
          RawKuduRecord record;
          record.ParseFromString(msg.get_payload());
          int32_t key = -1;
          string value;
          int32_t value_int = -2;
          for (int i = 0; i < record.properties_size(); i++) {
            const kafka::RawProperty& property = record.properties(i);
            if ("key" == property.name()) {
              ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &key));
              key_set.insert(key);
            }
            if ("string_val" == property.name()) {
              ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &value_int));
            }
          }
          ASSERT_EQ(2 * key, value_int);
          consumer_->commit(msg);
          count++;
        }
      }
    }

    messages = consumer_->poll_batch(max_batch_size, timeout);
    LOG(INFO) << Substitute("expect size: $0, real size: $1, count: $2, last message size: $3",
                            expected_size,
                            count + messages.size(),
                            count,
                            messages.size());
    ASSERT_GE(count, expected_size);
    ASSERT_EQ(expected_size, key_set.size());
  }

 protected:
  Status CreateTable(const CreateTableOptions& options) {
    std::unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name(options.table_name)
        .schema(&schema_)
        .num_replicas(options.replication_refactor);
    if (options.partition_num == 1) {
      table_creator->set_range_partition_columns({"key"});
    } else {
      table_creator->add_hash_partitions({"key"}, options.partition_num);
    }
    if (options.dup_info != std::nullopt) {
      table_creator->duplication(options.dup_info.value());
    }
    RETURN_NOT_OK(table_creator->Create());

    RETURN_NOT_OK(client_->OpenTable(options.table_name, &table_));
    return Status::OK();
  }

  Status AlterTable(const AlterTableOptions& options) {
    std::unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(options.table_name));
    if (options.action == Action::kEnableDuplication) {
      alterer->AddDuplicationInfo(options.info);
    } else {
      CHECK(Action::kDisableDuplication == options.action);
      alterer->DropDuplicationInfo(options.info);
    }
    RETURN_NOT_OK(alterer->Alter());
    return Status::OK();
  }

  Status DeleteTable(const string& table_name) { return client_->DeleteTable(table_name); }

  static void StatusCB(void* /* unused */, const Status& status) {
    KUDU_LOG(INFO) << "Asynchronous flush finished with status: " << status.ToString();
  }

  Status InsertRows(int from, int to) {
    client::sp::shared_ptr<client::KuduSession> session = table_->client()->NewSession();
    KUDU_RETURN_NOT_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(5000);
    SCOPED_CLEANUP({ WARN_NOT_OK(session->Flush(), "flush failed"); });

    static int batch_size = 64;
    int retry = 100;
    int current_batch_count = 0;
    Status status = Status::OK();
    for (int i = from; i < to; i++) {
      client::KuduInsert* insert = table_->NewInsert();
      KuduPartialRow* row = insert->mutable_row();
      KUDU_CHECK_OK(row->SetInt32("key", i));
      KUDU_CHECK_OK(row->SetInt32("int_val", i));
      KUDU_CHECK_OK(row->SetString("string_val", std::to_string(i * 2)));
      KUDU_CHECK_OK(session->Apply(insert));
      if (++current_batch_count == batch_size) {
        retry = 240;
        while (!(status = session->Flush()).ok() && retry-- >= 0) {
          VLOG(0) << "flush failed: " << status.ToString();
          SleepFor(MonoDelta::FromMilliseconds(1000));
        }
        RETURN_NOT_OK(status);
        current_batch_count = 0;
      }
    }
    retry = 10;
    while (!(status = session->Flush()).ok() && retry-- >= 0) {
      SleepFor(MonoDelta::FromMilliseconds(500));
    }
    return status;
  }

 public:
  std::shared_ptr<cppkafka::Consumer> consumer_;
  // kafka cluster.
  duplication::kafka::SingleBrokerKafka kafka_;

 private:
  cluster::ExternalMiniClusterOptions cluster_opts_;
  const client::KuduSchema schema_;
  client::sp::shared_ptr<client::KuduTable> table_;
  std::shared_ptr<rpc::Messenger> client_messenger_;
};

// Test CreateTable with duplication
TEST_F(DuplicationITest, CreateTableWithDuplicationAndTestDuplication) {
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  options.dup_info = dup_info;
  options.partition_num = 2;
  options.replication_refactor = 3;
  options.table_name = "CreateTableWithDuplication";
  ASSERT_OK(CreateTable(options));

  LOG(INFO) << "Will do InsertRows";
  // @TODO(duyuqi), patch updates, deletes, upsert ...
  // Write rows
  int insert_count = 128;
  int expected_size = insert_count;
  std::thread t(std::bind(&DuplicationITest::Consume,
                          this,
                          expected_size,
                          "CreateTableWithDuplicationAndTestDuplication",
                          kSmallest));
  SleepFor(MonoDelta::FromSeconds(1));
  ASSERT_OK(InsertRows(0, insert_count));
  t.join();
}

// Test wal' gc with duplication
TEST_F(DuplicationITest, CreateTableWithDuplicationAndTestWalGc) {
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "CreateTableWithDuplicationAndTestWalGc";
  ASSERT_OK(CreateTable(options));
  SleepFor(MonoDelta::FromSeconds(5));

  // It will produce 6 wal files or so.
  int insert_count = 300000;
  ASSERT_OK(InsertRows(0, insert_count));

  // Get the unique tablet_id.
  string tablet_id;
  client::sp::shared_ptr<client::KuduTable> client_table;
  ASSERT_OK(client_->OpenTable(options.table_name, &client_table));
  vector<client::KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  client::KuduScanTokenBuilder builder(client_table.get());
  ASSERT_OK(builder.Build(&tokens));
  for (const auto* token : tokens) {
    tablet_id = token->tablet().id();
    break;
  }

  // Check WAL gc at all tservers.
  string prefix = "wal-";
  int dir_count = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    // @TODO(duyuqi) Finish the test case.
    vector<string> result;
    string path = JoinPathSegments(cluster_->WalRootForTS(i), "wals");
    path = JoinPathSegments(path, tablet_id);
    Status status = (Env::Default()->GetChildren(path, &result));
    if (status.IsNotFound()) {
      continue;
    }
    dir_count++;

    // a case need 108s
    int32_t timeout_s = 300;
    ASSERT_EVENTUALLY_WITH_TIMEOUT([&]() {
      ASSERT_OK(Env::Default()->GetChildren(path, &result));
      int wal_num = 0;
      for (auto& r : result) {
        if (r == "." || r == "..") {
          continue;
        }
        Slice name(r);
        if (name.starts_with(prefix)) {
          wal_num++;
        }
      }
      ASSERT_LE(wal_num, 2);
    }, MonoDelta::FromSeconds(timeout_s));
  }
  ASSERT_EQ(options.replication_refactor, dir_count);
}

TEST_F(DuplicationITest, AlterTableWithDuplicationAndTestDuplication) {
  string kTableName = "AlterTableWithDuplication";
  CreateTableOptions options;
  options.partition_num = 2;
  options.replication_refactor = 3;
  options.table_name = kTableName;
  ASSERT_OK(CreateTable(options));

  AlterTableOptions alter_options;
  alter_options.table_name = kTableName;
  alter_options.info.name = kTopicName;
  alter_options.info.type = client::DuplicationDownstream::KAFKA;
  alter_options.info.uri = kBrokers;

  int insert_count = 128;
  int expected_size = insert_count;

  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));

  std::thread t(std::bind(&DuplicationITest::Consume,
                          this,
                          expected_size,
                          "AlterTableWithDuplicationAndTestDuplication",
                          kSmallest));
  SleepFor(MonoDelta::FromMilliseconds(2000));
  ASSERT_OK(InsertRows(0, insert_count));
  t.join();
  SleepFor(MonoDelta::FromMilliseconds(1000));
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationITest, AlterTableWithDuplicationFixedInvalidBroker) {
  string kTableName = "AlterTableWithDuplicationFixedInvalidBroker";
  CreateTableOptions options;
  options.partition_num = 2;
  options.replication_refactor = 3;
  options.table_name = kTableName;
  ASSERT_OK(CreateTable(options));

  AlterTableOptions alter_options;
  alter_options.table_name = kTableName;
  alter_options.info.name = kTopicName;
  alter_options.info.type = client::DuplicationDownstream::KAFKA;

  // "localhost:8888" is a invalid kafka brokers.
  alter_options.info.uri = "localhost:8888";
  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  SleepFor(MonoDelta::FromSeconds(1));
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  SleepFor(MonoDelta::FromSeconds(1));

  alter_options.info.uri = kBrokers;
  int insert_count = 128;
  int expected_size = insert_count;
  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));

  std::thread t(std::bind(&DuplicationITest::Consume,
                          this,
                          expected_size,
                          "AlterTableWithDuplicationFixedInvalidBroker",
                          kSmallest));
  ASSERT_OK(InsertRows(0, insert_count));
  t.join();
  SleepFor(MonoDelta::FromMilliseconds(1000));
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationITest, DuplicatorRecovering) {
  string kTableName = "DuplicatorRecovering";
  CreateTableOptions options;
  options.partition_num = 2;
  options.replication_refactor = 3;
  options.table_name = kTableName;
  ASSERT_OK(CreateTable(options));

  int insert_count = 256;
  int second_count = 32;
  int expected_size = insert_count + second_count;
  ASSERT_OK(InsertRows(0, insert_count));

  std::thread t(&DuplicationITest::Consume, this, expected_size, "DuplicatorRecovering", kSmallest);
  SleepFor(MonoDelta::FromMilliseconds(1000));

  AlterTableOptions alter_options;
  alter_options.table_name = kTableName;
  alter_options.info.name = kTopicName;
  alter_options.info.type = client::DuplicationDownstream::KAFKA;
  alter_options.info.uri = kBrokers;


  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  SleepFor(MonoDelta::FromMilliseconds(1000));
  ASSERT_OK(InsertRows(insert_count, expected_size));
  t.join();
  SleepFor(MonoDelta::FromMilliseconds(1000));
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationITest, RestartTserver) {
  // RestartTserver
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "RestartTserver";
  ASSERT_OK(CreateTable(options));

  // @TODO(duyuqi), patch updates, deletes, upsert, Write rows
  int insert_count = 20000;
  int expected_size = insert_count;
  std::thread consume_thread(
      std::bind(&DuplicationITest::Consume, this, expected_size, "RestartTserver", kSmallest));
  std::thread write_thread(&DuplicationITest::WriteRows, this, 0, insert_count);
  SleepFor(MonoDelta::FromSeconds(2));
  // for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster::ExternalTabletServer* ts = cluster_->tablet_server(i);
    ts->Shutdown();
    CHECK_OK(ts->Restart());
    SleepFor(MonoDelta::FromSeconds(5));
  }
  // cluster::ExternalTabletServer* ts = cluster_->tablet_server(0);
  // ts->Shutdown();
  // VLOG(0) << "Start Start Start Start Start Start Start";
  // WARN_NOT_OK(ts->Restart(), "restart not ok");
  SleepFor(MonoDelta::FromSeconds(10));

  write_thread.join();
  consume_thread.join();
}

TEST_F(DuplicationITest, DuplicatorKafkaDownAndCheck) {
  FLAGS_kafka_connector_flush_timeout_ms = 1000;
  FLAGS_duplicator_max_queue_size = 64;

  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "KafkaDownAndRecover";
  ASSERT_OK(CreateTable(options));
  SleepFor(MonoDelta::FromSeconds(5));
  int pre_count = 1000;
  int insert_count = 1000000;
  WriteRows(0, pre_count);
  kafka_.StopKafka();
  std::thread write_thread(&DuplicationITest::WriteRows, this, pre_count + 1, insert_count);
  write_thread.join();
  VLOG(0) << "Insert finish";
  SleepFor(MonoDelta::FromSeconds(3600));
  VLOG(0) << "Test finish";
}

TEST_F(DuplicationITest, DuplicatorKafkaDownAndRecover) {
  FLAGS_kafka_connector_flush_timeout_ms = 1000;
  FLAGS_duplicator_max_queue_size = 64;

  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "KafkaDownAndRecover";
  ASSERT_OK(CreateTable(options));

  // Make kafka stop 5s
  SleepFor(MonoDelta::FromSeconds(2));
  kafka_.StopKafka();
  int insert_count = 10000;
  int expected_size = insert_count;
  std::thread consume_thread(std::bind(
      &DuplicationITest::Consume, this, expected_size, "DuplicatorKafkaDownAndRecover", kSmallest));

  std::thread write_thread(&DuplicationITest::WriteRows, this, 0, insert_count);
  SleepFor(MonoDelta::FromSeconds(10));
  kafka_.StartKafka();

  write_thread.join();
  consume_thread.join();
}

}  // namespace kudu
