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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
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
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/value.h"
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
DECLARE_uint32(switch_to_leader_checker_internal_ms);

using std::set;
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
  const int kOffsetPort = 9;
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
    cluster_opts_.extra_tserver_flags.emplace_back("--enable_wait_producer_flush_for_testing=true");
    cluster_opts_.extra_tserver_flags.emplace_back("--duplicator_max_queue_size=64");
  }
  void SetUp() override {
    NO_FATALS(StartClusterWithOpts(cluster_opts_));
    kafka_.UninstallKafka();
    kafka_.InstallKafka();
  }
  void TearDown() override {
    ExternalMiniClusterITestBase::TearDown();
    if (consumer_) {
      consumer_->unsubscribe();
      consumer_.reset();
    }
    kafka_.UninstallKafka();
  }

  void WriteRows(int from, int to) {
    MonoTime now = MonoTime::Now();
    int next = from;
    int retry_index = 0;
    Status status = InsertRows(next, to, &next);
    while (!status.ok() && next < to - 1) {
      status = InsertRows(next, to, &next);
      LOG_IF(ERROR, !status.ok()) << "InsertRows, error, retry_index: " << retry_index;
      ++retry_index;
    }
    MonoDelta delta = MonoTime::Now() - now;
    LOG(INFO) << "WriteRows cost: " << delta.ToString() << ", retry_index: " << retry_index;
  }

  Status TryAlterSchema(const string& table_name, bool* success) {
    *success = false;
    string kColumnName("TestAddColumeAlter");
    string kColumnName2("TestAddColumeAlter2");
    {
      std::unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(table_name));
      client::KuduValue* value = client::KuduValue::FromInt(0);
      alterer->AddColumn(kColumnName)->Type(client::KuduColumnSchema::INT32)->Default(value);
      RETURN_NOT_OK(alterer->Alter());
    }
    SleepFor(MonoDelta::FromSeconds(1));
    {
      std::unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(table_name));
      alterer->AlterColumn(kColumnName)->RenameTo(kColumnName2);
      RETURN_NOT_OK(alterer->Alter());
    }
    SleepFor(MonoDelta::FromSeconds(1));
    {
      std::unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(table_name));
      alterer->DropColumn(kColumnName2);
      RETURN_NOT_OK(alterer->Alter());
    }
    *success = true;
    return Status::OK();
  }

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

  void Consume(int expected_size,
               const string& group_name,
               const string& strategy,
               int32_t timeout_s,
               std::set<int32_t>* key_set) {
    cppkafka::Configuration configuration = {{"metadata.broker.list", kBrokers},
                                             {"group.id", group_name},
                                             {"enable.auto.commit", false},
                                             {"auto.offset.reset", strategy}};

    RenewConsumer(configuration);

    std::chrono::milliseconds timeout(1000);
    int count = 0;
    constexpr const int max_batch_size = 16;
    vector<cppkafka::Message> messages;
    int noop_wait_s = 0;
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
        VLOG(0) << Substitute("kafka consumer poll messages is empty, wait $0s", noop_wait_s);
        if (noop_wait_s++ >= timeout_s || (noop_wait_s > 10 && key_set->size() == expected_size)) {
          VLOG(0) << "Finish consume kafka, noop_wait_s: " << noop_wait_s;
          break;
        }
        continue;
      }
      noop_wait_s = 0;
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
              key_set->insert(key);
            }
            if ("string_val" == property.name()) {
              ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &value_int));
            }
          }
          ASSERT_EQ(2 * key, value_int);
          int retry_total_count = 1000;
          while (--retry_total_count < 0) {
            try {
              consumer_->commit(msg);
              break;
            } catch (const cppkafka::HandleException& e) {
              LOG(WARNING) << "consumer commit: " << e.what();
              SleepFor(MonoDelta::FromSeconds(500));
            }
          }
          if (retry_total_count < 0) {
            LOG(ERROR) << "consumer commit error, retries: 500s" << record.ShortDebugString();
          }
          count++;
        }
      }
    }

    messages = consumer_->poll_batch(max_batch_size, timeout);
    LOG(INFO) << Substitute("consume record size: $0, count: $1, last message size: $2",
                            count + messages.size(),
                            count,
                            messages.size());
  }

  static void ScanRows(const client::sp::shared_ptr<client::KuduTable>& table,
                       set<int32_t>* key_set) {
    CHECK(key_set);
    client::KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(scanner.SetSelection(client::KuduClient::ReplicaSelection::LEADER_ONLY));
    ASSERT_OK(scanner.Open());
    client::KuduScanBatch batch;

    ASSERT_TRUE(scanner.HasMoreRows());
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      for (client::KuduScanBatch::const_iterator it = batch.begin(); it != batch.end();
           ++it) {
        client::KuduScanBatch::RowPtr row(*it);
        int32_t key = -1;
        int32_t int_val = -1;
        Slice slice_val;

        ASSERT_OK(row.GetInt32("key", &key));
        ASSERT_OK(row.GetInt32("int_val", &int_val));
        ASSERT_OK(row.GetString("string_val", &slice_val));
        int32_t string_val_int;
        ASSERT_TRUE(SimpleAtoi(slice_val.ToString().c_str(), &string_val_int));
        ASSERT_EQ(key, int_val);
        ASSERT_EQ(string_val_int, 2 * key);
        key_set->insert(key);
      }
    }
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

  Status InsertRows(int from, int to, int* next = nullptr) {
    client::sp::shared_ptr<client::KuduSession> session = table_->client()->NewSession();
    KUDU_RETURN_NOT_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(5000);
    SCOPED_CLEANUP({ WARN_NOT_OK(session->Flush(), "flush failed"); });

    static int batch_size = 16;
    int retry = 100;
    int current_batch_count = 0;
    set<int32_t> key_set;
    Status status = Status::OK();
    for (int i = from; i < to; i++) {
      if (next) {
        *next = i;
      }
      client::KuduInsert* insert = table_->NewInsert();
      KuduPartialRow* row = insert->mutable_row();
      KUDU_CHECK_OK(row->SetInt32("key", i));
      KUDU_CHECK_OK(row->SetInt32("int_val", i));
      KUDU_CHECK_OK(row->SetString("string_val", std::to_string(i * 2)));
      KUDU_CHECK_OK(session->Apply(insert));
      key_set.insert(i);
      if (++current_batch_count == batch_size) {
        retry = 20;
        while (!(status = session->Flush()).ok() && retry-- >= 0) {
          VLOG(0) << i << " flush failed: " << status.ToString();
          SleepFor(MonoDelta::FromMilliseconds(1000));
        }
        RETURN_NOT_OK(status);
        key_set.clear();
        current_batch_count = 0;
      }
    }
    retry = 10;
    while (!(status = session->Flush()).ok() && retry-- >= 0) {
      SleepFor(MonoDelta::FromMilliseconds(500));
      VLOG(0) << "last flush failed: " << status.ToString();
    }
    return status;
  }

 public:
  std::shared_ptr<cppkafka::Consumer> consumer_;
  // kafka cluster.
  duplication::kafka::SingleBrokerKafka kafka_;
  client::sp::shared_ptr<client::KuduTable> table_;

 private:
  cluster::ExternalMiniClusterOptions cluster_opts_;
  const client::KuduSchema schema_;
  std::shared_ptr<rpc::Messenger> client_messenger_;
};

// Test CreateTable with duplication
TEST_F(DuplicationITest, CreateTableWithDuplicationAndTestDuplication) {
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  dup_info.kerberos_options = nullptr;
  options.dup_info = dup_info;
  options.partition_num = 2;
  options.replication_refactor = 3;
  options.table_name = "CreateTableWithDuplication";
  ASSERT_OK(CreateTable(options));

  // @TODO(duyuqi), patch updates, deletes, upsert ...
  // Write rows
  int insert_count = 128;
  int expected_size = insert_count;
  set<int32_t> kafka_key_set;
  std::thread t(std::bind(&DuplicationITest::Consume,
                          this,
                          expected_size,
                          "CreateTableWithDuplicationAndTestDuplication",
                          kSmallest, 20, &kafka_key_set));
  ASSERT_OK(InsertRows(0, insert_count));
  t.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
}

// Test Write Two same primary key record with duplication
TEST_F(DuplicationITest, InsertTwoSamePrimaryKeyRecord) {
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  dup_info.kerberos_options = nullptr;
  options.dup_info = dup_info;
  options.partition_num = 2;
  options.replication_refactor = 3;
  options.table_name = "InsertTwoSameRecord";
  ASSERT_OK(CreateTable(options));
  SleepFor(MonoDelta::FromSeconds(5));

  client::sp::shared_ptr<client::KuduSession> session = table_->client()->NewSession();
  KUDU_CHECK_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  Status status = Status::OK();
  int i = 1;
  {
    client::KuduInsert* insert = table_->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    CHECK_OK(row->SetInt32("key", i));
    CHECK_OK(row->SetInt32("int_val", i));
    CHECK_OK(row->SetString("string_val", std::to_string(i * 2)));
    CHECK_OK(session->Apply(insert));
    status = session->Flush();
    ASSERT_OK(status);
  }
  {
    client::KuduInsert* insert = table_->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    CHECK_OK(row->SetInt32("key", i));
    CHECK_OK(row->SetInt32("int_val", i * 2));
    CHECK_OK(row->SetString("string_val", std::to_string(i * 2 * 2)));
    CHECK_OK(session->Apply(insert));
    status = session->Flush();
    ASSERT_FALSE(status.ok());
  }

  std::chrono::milliseconds timeout_ms(1000);
  cppkafka::Configuration configuration = {{"metadata.broker.list", kBrokers},
                                           {"group.id", options.table_name},
                                           {"enable.auto.commit", false},
                                           {"auto.offset.reset", kSmallest}};
  RenewConsumer(configuration);
  vector<cppkafka::Message> messages;
  int retry = 20;
  while (true) {
    messages.clear();
    try {
      messages = consumer_->poll_batch(64, timeout_ms);
    } catch (const cppkafka::Exception& e) {
        LOG(WARNING) << "consumer exception: " << e.what();
        SleepFor(MonoDelta::FromMilliseconds(100));
        continue;
    }
    if (!messages.empty() || retry-- < 0) {
      break;
    }
    SleepFor(MonoDelta::FromSeconds(1));
  }
  ASSERT_EQ(1, messages.size());
  cppkafka::Message& msg = messages[0];
  RawKuduRecord record;
  record.ParseFromString(msg.get_payload());
  int32_t key = -1;
  int32_t value_string = -5;
  int32_t value_int = -2;
  for (int i = 0; i < record.properties_size(); i++) {
    const kafka::RawProperty& property = record.properties(i);
    if ("key" == property.name()) {
      ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &key));
    }
    if ("string_val" == property.name()) {
      ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &value_string));
    }
    if ("int_val" == property.name()) {
      ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &value_int));
    }
  }
  ASSERT_EQ(i, key);
  ASSERT_EQ(i, value_int);
  ASSERT_EQ(i * 2, value_string);
}

// Test wal' gc with duplication
TEST_F(DuplicationITest, CreateTableWithDuplicationAndTestWalGc) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  {
    // TODO(duyuqi)
    LOG(INFO) << "MockExecute. This test is very slow, so I plan move to other code path. Skip "
                 "this test.";
    GTEST_SKIP();
  }
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  dup_info.kerberos_options = nullptr;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "CreateTableWithDuplicationAndTestWalGc";
  ASSERT_OK(CreateTable(options));
  SleepFor(MonoDelta::FromSeconds(5));

  // It will produce 15 wal files or so.
  int insert_count = 1200000;
  WriteRows(0, insert_count);

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
  int next_id = insert_count + 1;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
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
    ASSERT_EVENTUALLY_WITH_TIMEOUT(
        [&]() {
          // TODO(duyuqi)
          // In a known case, writing kudu is too quick and duplicate to kafka is too slow.
          // Duplication will be downgraded to wal duplication state. But the duplicated progress
          // is not replicate to followers because no write requests incoming, only normal raft
          // replication can transfer that duplicated point. So at this case, we write 1 row to
          // duplicate the duplicated progress, at Realtime duplication state, this is just a
          // temporary fix.
          //
          // In fact, this case need a more normally fix. Duplicator's committed index can be
          // removed? And heartbeat in raft group should carry this point to followers? Or we
          // should add a new rpc(Submit a DuplicateOp) to transfer this progress to followers
          // after replaying this wals for duplication?
          //
          // This case will be fixed in future.
          WriteRows(next_id, next_id + 1);
          next_id++;
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
        },
        MonoDelta::FromSeconds(timeout_s));
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
  alter_options.info.kerberos_options = nullptr;

  int insert_count = 128;
  int expected_size = insert_count;

  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));

  set<int32_t> kafka_key_set;
  std::thread t(std::bind(&DuplicationITest::Consume,
                          this,
                          expected_size,
                          "AlterTableWithDuplicationAndTestDuplication",
                          kSmallest,
                          20, &kafka_key_set));
  SleepFor(MonoDelta::FromSeconds(2));
  ASSERT_OK(InsertRows(0, insert_count));
  t.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationITest, AlterTableWithDuplicationBeforeKafkaNormal) {
  string kTableName = "AlterTableWithDuplicationBeforeKafkaNormal";
  CreateTableOptions options;
  options.partition_num = 2;
  options.replication_refactor = 3;
  options.table_name = kTableName;
  ASSERT_OK(CreateTable(options));

  kafka_.StopBrokers();
  AlterTableOptions alter_options;
  alter_options.table_name = kTableName;
  alter_options.info.name = kTopicName;
  alter_options.info.type = client::DuplicationDownstream::KAFKA;
  alter_options.info.uri = kBrokers;
  alter_options.info.kerberos_options = nullptr;

  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationITest, AlterTableWithDuplicationFixedInvalidBroker) {
  SKIP_IF_SLOW_NOT_ALLOWED();
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
  alter_options.info.kerberos_options = nullptr;

  // "localhost:8888" is a invalid kafka brokers.
  alter_options.info.uri = "localhost:8888";
  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_switch_to_leader_checker_internal_ms));
  int insert_count = 10;
  ASSERT_OK(InsertRows(0, insert_count));

  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  SleepFor(MonoDelta::FromSeconds(1));

  alter_options.info.uri = kBrokers;
  int accumulate_insert_count = 128;
  int expected_size = accumulate_insert_count;
  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));

  set<int32_t> kafka_key_set;
  std::thread t(std::bind(&DuplicationITest::Consume,
                          this,
                          expected_size,
                          "AlterTableWithDuplicationFixedInvalidBroker",
                          kSmallest,
                          20, &kafka_key_set));
  ASSERT_OK(InsertRows(insert_count, accumulate_insert_count));
  t.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
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

  set<int32_t> kafka_key_set;
  std::thread t(&DuplicationITest::Consume,
                this,
                expected_size,
                "DuplicatorRecovering",
                kSmallest,
                20,
                &kafka_key_set);
  SleepFor(MonoDelta::FromMilliseconds(1000));

  AlterTableOptions alter_options;
  alter_options.table_name = kTableName;
  alter_options.info.name = kTopicName;
  alter_options.info.type = client::DuplicationDownstream::KAFKA;
  alter_options.info.uri = kBrokers;
  alter_options.info.kerberos_options = nullptr;

  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  SleepFor(MonoDelta::FromMilliseconds(1000));
  ASSERT_OK(InsertRows(insert_count, expected_size));
  t.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
  SleepFor(MonoDelta::FromMilliseconds(1000));
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationITest, RestartTserver) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  dup_info.kerberos_options = nullptr;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "RestartTserver";
  ASSERT_OK(CreateTable(options));

  // @TODO(duyuqi), patch updates, deletes, upsert, Write rows
  int insert_count = 20000;
  int expected_size = insert_count;
  set<int32_t> kafka_key_set;
  std::thread consume_thread(std::bind(&DuplicationITest::Consume,
                                       this,
                                       expected_size,
                                       options.table_name,
                                       kSmallest,
                                       20,
                                       &kafka_key_set));
  std::thread write_thread(&DuplicationITest::WriteRows, this, 0, insert_count);
  for (int i = 0; i < std::min(3, cluster_->num_tablet_servers()); i++) {
    cluster::ExternalTabletServer* ts = cluster_->tablet_server(i);
    ts->Shutdown();
    ASSERT_OK(ts->Restart());
    SleepFor(MonoDelta::FromSeconds(3));
  }
  SleepFor(MonoDelta::FromSeconds(1));

  write_thread.join();
  consume_thread.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
  kafka_key_set.clear();
  Consume(expected_size, options.table_name + "1", kSmallest, 20, &kafka_key_set);
  ASSERT_EQ(expected_size, kafka_key_set.size());
}

TEST_F(DuplicationITest, DuplicatorKafkaDownAndCheckKuduWriteOKAndAlterSchemaOk) {
  FLAGS_kafka_connector_flush_timeout_ms = 1000;
  FLAGS_duplicator_max_queue_size = 64;

  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  dup_info.kerberos_options = nullptr;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "DuplicatorKafkaDownAndCheckKuduWriteOK";
  ASSERT_OK(CreateTable(options));

  int pre_count = 2000;
  int insert_count = 20000;
  WriteRows(0, pre_count);
  kafka_.StopBrokers();
  std::thread write_thread(&DuplicationITest::WriteRows, this, pre_count, insert_count);
  bool success = false;
  std::thread alter_thread(&DuplicationITest::TryAlterSchema, this, options.table_name, &success);
  // Check Kudu data is 20000 rows.
  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(options.table_name, &table));
  ASSERT_EVENTUALLY_WITH_TIMEOUT(
      [&]() {
        std::set<int32_t> key_set;
        ScanRows(table, &key_set);
        ASSERT_EQ(static_cast<uint32_t>(insert_count), key_set.size());
      },
      MonoDelta::FromSeconds(120));
  write_thread.join();
  alter_thread.join();
  ASSERT_EQ(true, success);
}

TEST_F(DuplicationITest, DuplicatorKafkaDownAndRecover) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  FLAGS_kafka_connector_flush_timeout_ms = 1000;
  FLAGS_duplicator_max_queue_size = 64;
  constexpr const int session_timeout_s = 60;

  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  dup_info.kerberos_options = nullptr;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "DuplicatorKafkaDownAndRecover";
  ASSERT_OK(CreateTable(options));

  int pre_count = 10000;
  int insert_count = 100000;
  int expected_size = insert_count;
  WriteRows(0, pre_count);
  // Check Kudu data is 1000 rows.
  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(options.table_name, &table));
  ASSERT_EVENTUALLY([&]() {
    std::set<int32_t> key_set;
    ScanRows(table, &key_set);
    ASSERT_EQ(static_cast<uint32_t>(pre_count), key_set.size());
  });

  kafka_.StopBrokers();
  set<int32_t> kafka_key_set;
  std::thread consume_thread(std::bind(&DuplicationITest::Consume,
                                       this,
                                       expected_size,
                                       options.table_name,
                                       kSmallest,
                                       session_timeout_s, &kafka_key_set));

  std::thread write_thread(&DuplicationITest::WriteRows, this, pre_count, insert_count);
  // To make sure kafka start success.
  SleepFor(MonoDelta::FromSeconds(
      2 * kudu::duplication::kafka::SingleBrokerKafka::kZookeeperSessionTimeoutS));
  kafka_.StartBrokers();

  write_thread.join();
  // Check Kudu data is 20000 rows.
  ASSERT_EVENTUALLY([&]() {
    std::set<int32_t> key_set;
    ScanRows(table, &key_set);
    ASSERT_EQ(static_cast<uint32_t>(insert_count), key_set.size());
  });
  consume_thread.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
  kafka_key_set.clear();
  DuplicationITest::Consume(
      expected_size, options.table_name + "1", kSmallest, session_timeout_s, &kafka_key_set);
  ASSERT_EQ(expected_size, kafka_key_set.size());
}

}  // namespace kudu
