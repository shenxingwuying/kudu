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

#include <cppkafka/buffer.h>
#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cppkafka/error.h>
#include <cppkafka/message.h>
#include <cppkafka/topic_partition_list.h>
#include <cstdint>
#include <cstdlib>

#include <chrono>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/callbacks.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace rpc {
class Messenger;
}  // namespace rpc

struct CreateTableOptions {
  std::string table_name;
  int32_t partition_num;
  int32_t replica_refactor;
  client::DuplicationInfo dup_info;
};

enum class Action {
  kEnableDuplication = 0,
  kDisableDuplication
};

struct AlterTableOptions {
  Action action;
  std::string table_name;
  client::DuplicationInfo info;
};
// Simple base utility class to provide an external mini cluster with common
// setup routines useful for integration tests. And start kafka service to test
// data duplication.
class DuplicationITest : public ExternalMiniClusterITestBase {
public:
  DuplicationITest() : schema_(SimpleKuduSchema()) {
    cluster_opts_.num_tablet_servers = 4;
    cluster_opts_.enable_duplication = true;

    // cluster_opts_.extra_master_flags.emplace_back("--v=64");
    // cluster_opts_.extra_tserver_flags.emplace_back("--v=64");
    options_.partition_num = 4;
    options_.replica_refactor = 3;
    options_.table_name = "duplication_table";
  }
  void SetUp() override {
    NO_FATALS(StartClusterWithOpts(cluster_opts_));
    DestroyKafka();
    InitKafka();
  }
  void TearDown() override {
    ExternalMiniClusterITestBase::TearDown();
    DestroyKafka();
  }


  void Consume(int expected_size) {
    std::chrono::milliseconds timeout(5000);
    int count = 0;
    int max_batch_size = 4;
    std::vector<cppkafka::Message> messages;
    while (true) {
      messages.clear();
      messages = consumer_->poll_batch(max_batch_size, timeout);
      for (const auto& msg : messages) {
        if (msg) {
          if (msg.get_error()) {
            if (!msg.is_eof()) {
              LOG(INFO) << "Received error notification: " << msg.get_error();
            }
          } else {
            if (msg.get_key()) {
              LOG(INFO) << "kafka key: " << msg.get_key();
            }
            LOG(INFO) << "kafka payload: " << msg.get_payload();
            consumer_->commit(msg);
            count++;
          }
        }
      }
      if (expected_size == count) {
        break;
      }
    }

    messages.clear();
    messages = consumer_->poll_batch(max_batch_size, timeout);
    ASSERT_TRUE(messages.empty());
  }

protected:
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

  Status CreateTable() {
    return CreateTable(options_, false);
  }

  Status CreateTable(const CreateTableOptions& options, bool has_duplication) {
    std::unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
    if (has_duplication) {
      RETURN_NOT_OK(table_creator->table_name(options.table_name)
                    .schema(&schema_)
                    .add_hash_partitions({ "key" }, options.partition_num)
                    .num_replicas(options.replica_refactor)
                    .duplication(options.dup_info)
                    .Create());
    } else {
      RETURN_NOT_OK(table_creator->table_name(options.table_name)
                    .schema(&schema_)
                    .add_hash_partitions({ "key" }, options.partition_num)
                    .num_replicas(options.replica_refactor)
                    .Create());
    }

    RETURN_NOT_OK(client_->OpenTable(options.table_name, &table_));
    bool is_creating = true;
    while (is_creating) {
      client_->IsCreateTableInProgress(options.table_name, &is_creating);
      SleepFor(MonoDelta::FromMilliseconds(1000));
    }
    LOG(INFO) << "create table "<< options.table_name << " done";
    return Status::OK();
  }

  Status AlterTable(const AlterTableOptions& options) {
    client::KuduTableAlterer* table_alterer = client_->NewTableAlterer(options.table_name);
    if (options.action == Action::kEnableDuplication) {
      table_alterer->AddDuplicationInfo(options.info);
    } else {
      table_alterer->DropDuplicationInfo(options.info);
    }
    RETURN_NOT_OK(table_alterer->Alter());
    delete table_alterer;
    bool is_altering = true;
    while (is_altering) {
      client_->IsAlterTableInProgress(options.table_name, &is_altering);
      SleepFor(MonoDelta::FromMilliseconds(1000));
    }
    LOG(INFO) << "alter table "<< options.table_name << " done";
    return Status::OK();
  }

  Status DeleteTable(const std::string& table_name) {
    return client_->DeleteTable(table_name);
  }

  static void StatusCB(void* /* unused */, const Status& status) {
    KUDU_LOG(INFO) << "Asynchronous flush finished with status: "
                  << status.ToString();
  }

  Status InsertRows(int num_rows) {
    client::sp::shared_ptr<client::KuduSession> session = table_->client()->NewSession();
    KUDU_RETURN_NOT_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(5000);

    for (int i = 0; i < num_rows; i++) {
      client::KuduInsert* insert = table_->NewInsert();
      KuduPartialRow* row = insert->mutable_row();
      KUDU_CHECK_OK(row->SetInt32("key", i));
      KUDU_CHECK_OK(row->SetString("value_string", std::to_string(i * 2)));
      // KUDU_CHECK_OK(row->SetInt64("value_string", static_cast<int64_t>(i * 3)));
      KUDU_CHECK_OK(session->Apply(insert));
    }
    Status s = session->Flush();
    if (s.ok()) {
      return s;
    }

    // Test asynchronous flush.
    client::KuduStatusFunctionCallback<void*> status_cb(&StatusCB, NULL);
    session->FlushAsync(&status_cb);

    // Look at the session's errors.
    std::vector<client::KuduError*> errors;
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    if (!errors.empty()) {
      s = overflow ? Status::IOError("Overflowed pending errors in session") :
          errors.front()->status();
      while (!errors.empty()) {
        delete errors.back();
        errors.pop_back();
      }
    }
    KUDU_RETURN_NOT_OK(s);

    // Close the session.
    return session->Close();
  }

private:
  static client::KuduSchema SimpleKuduSchema() {
    client::KuduSchema s;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("value_string")->Type(client::KuduColumnSchema::STRING)->NotNull();

    // add column by alter schema
    // b.AddColumn("value_int64")->Type(client::KuduColumnSchema::INT64);
    CHECK_OK(b.Build(&s));
    return s;
  }

public:
  std::shared_ptr<cppkafka::Consumer> consumer_;

private:
  cluster::ExternalMiniClusterOptions cluster_opts_;

  const client::KuduSchema schema_;
  client::sp::shared_ptr<client::KuduTable> table_;
  std::shared_ptr<rpc::Messenger> client_messenger_;

  CreateTableOptions options_;
};

// regression testing for CreateTable
TEST_F(DuplicationITest, CreateTable) {
  ASSERT_OK(CreateTable());
}

// Test CreateTable with duplication
TEST_F(DuplicationITest, CreateTableWithDuplicationAndTestDuplication) {
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = "create_table_topic";
  dup_info.type = client::DuplicationDownstream::KAFKA;
  options.dup_info = dup_info;
  options.partition_num = 2;
  options.replica_refactor = 3;
  options.table_name = "CreateTableWithDuplication";
  ASSERT_OK(CreateTable(options, true));

  // @TODO(duyuqi), patch updates, deletes, upsert ...
  // Write rows
  int insert_count = 128;
  int expected_size = insert_count;
  std::thread t(&DuplicationITest::Consume, this, expected_size);
  SleepFor(MonoDelta::FromMilliseconds(2000));
  ASSERT_OK(InsertRows(insert_count));
  t.join();
}

TEST_F(DuplicationITest, AlterTableWithDuplicationAndTestDuplication) {
  std::string kTableName = "AlterTableWithDuplication";
  CreateTableOptions options;
  options.partition_num = 2;
  options.replica_refactor = 3;
  options.table_name = kTableName;
  ASSERT_OK(CreateTable(options, false));

  AlterTableOptions alter_options;
  alter_options.table_name = kTableName;
  // @TODO(duyuqi)
  alter_options.info.name = "hello";
  alter_options.info.type = client::DuplicationDownstream::KAFKA;

  int insert_count = 128;
  int expected_size = insert_count;

  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));

  std::thread t(&DuplicationITest::Consume, this, expected_size);
  SleepFor(MonoDelta::FromMilliseconds(2000));
  ASSERT_OK(InsertRows(insert_count));
  t.join();
  SleepFor(MonoDelta::FromMilliseconds(1000));
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationITest, DuplicatorRecovering) {
  std::string kTableName = "DuplicatorRecovering";
  CreateTableOptions options;
  options.partition_num = 2;
  options.replica_refactor = 3;
  options.table_name = kTableName;
  ASSERT_OK(CreateTable(options, false));

  int insert_count = 128;
  int expected_size = insert_count;
  ASSERT_OK(InsertRows(insert_count));

  std::thread t(&DuplicationITest::Consume, this, expected_size);

  AlterTableOptions alter_options;
  alter_options.table_name = kTableName;
  // @TODO(duyuqi)
  alter_options.info.name = "hello";
  alter_options.info.type = client::DuplicationDownstream::KAFKA;

  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));
  SleepFor(MonoDelta::FromMilliseconds(2000));
  t.join();
  SleepFor(MonoDelta::FromMilliseconds(1000));

  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}


} // namespace kudu

