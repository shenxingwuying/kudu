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
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/single_broker_kafka.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

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
class DuplicationWithKerberosKafkaITest : public ExternalMiniClusterITestBase {
 public:
  // default topic.
  const string kTopicName = "kudu_profile_record_stream";
  // default kafka uri(brokers).
  const int kOffsetPort = 12;
  const string kBrokers =
      Substitute("localhost:$0", duplication::kafka::kKafkaBasePort + kOffsetPort);

  const string kSmallest = "smallest";
  const string kLargest = "largest";

  DuplicationWithKerberosKafkaITest()
      : kafka_(kOffsetPort), schema_(client::KuduSchema::FromSchema(GetSimpleTestSchema())) {
    cluster_opts_.num_tablet_servers = 4;
    cluster_opts_.enable_kerberos = true;
    cluster_opts_.extra_tserver_flags.emplace_back("--log_segment_size_mb=1");
    cluster_opts_.extra_tserver_flags.emplace_back("--enable_wait_producer_flush_for_testing=true");
    cluster_opts_.extra_tserver_flags.emplace_back("--duplicator_max_queue_size=64");
  }
  void SetUp() override {
    NO_FATALS(StartClusterWithOpts(cluster_opts_));
    kafka_principal_ = "kafka/localhost@KRBTEST.COM";
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(kafka_principal_));
    ASSERT_OK(cluster_->kdc()->CreateKeytabForExistingPrincipal(kafka_principal_));
    kafka_keytab_file_ = cluster_->kdc()->GetKeytabPathForPrincipal(kafka_principal_);
    while (kafka_.Alive()) {
      LOG(INFO) << "kafka is alive, should stop and destroy it";
      kafka_.UninstallKafka();
      SleepFor(MonoDelta::FromSeconds(1));
    }
    kafka_.InstallKafka(kafka_keytab_file_, kafka_principal_);
  }
  void TearDown() override {
    if (consumer_) {
      consumer_->unsubscribe();
      consumer_.reset();
    }
    kafka_.UninstallKafka();
    ExternalMiniClusterITestBase::TearDown();
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
    configuration.set("security.protocol", "SASL_PLAINTEXT");
    configuration.set("sasl.kerberos.service.name", "kafka");
    configuration.set("sasl.kerberos.keytab", kafka_keytab_file_);
    configuration.set("sasl.kerberos.principal", kafka_principal_);

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
  std::string kafka_keytab_file_;
  std::string kafka_principal_;
  duplication::kafka::SingleBrokerKafka kafka_;
  client::sp::shared_ptr<client::KuduTable> table_;

 private:
  cluster::ExternalMiniClusterOptions cluster_opts_;
  const client::KuduSchema schema_;
  std::shared_ptr<rpc::Messenger> client_messenger_;
};

// Test CreateTable with duplication
TEST_F(DuplicationWithKerberosKafkaITest, CreateTableWithDuplicationAndTestDuplication) {
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;

  kudu::KerberosOptions kerberos_options;
  kerberos_options.set_security_protocol(kudu::KerberosOptions::SASL_PLAINTEXT);
  kerberos_options.set_service_name("kafka");
  kerberos_options.set_keytab(kafka_keytab_file_);
  kerberos_options.set_principal(kafka_principal_);
  dup_info.kerberos_options = &kerberos_options;

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
  std::thread t(std::bind(&DuplicationWithKerberosKafkaITest::Consume,
                          this,
                          expected_size,
                          "CreateTableWithDuplicationAndTestDuplication",
                          kSmallest, 20, &kafka_key_set));
  ASSERT_OK(InsertRows(0, insert_count));
  t.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
}

TEST_F(DuplicationWithKerberosKafkaITest, AlterTableWithDuplicationAndTestDuplication) {
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

  kudu::KerberosOptions kerberos_options;
  kerberos_options.set_security_protocol(kudu::KerberosOptions::SASL_PLAINTEXT);
  kerberos_options.set_service_name("kafka");
  kerberos_options.set_keytab(kafka_keytab_file_);
  kerberos_options.set_principal(kafka_principal_);
  alter_options.info.kerberos_options = &kerberos_options;

  int insert_count = 128;
  int expected_size = insert_count;

  alter_options.action = Action::kEnableDuplication;
  ASSERT_OK(AlterTable(alter_options));

  set<int32_t> kafka_key_set;
  std::thread t(std::bind(&DuplicationWithKerberosKafkaITest::Consume,
                          this,
                          expected_size,
                          "AlterTableWithDuplicationAndTestDuplication",
                          kSmallest,
                          20, &kafka_key_set));
  ASSERT_OK(InsertRows(0, insert_count));
  t.join();
  ASSERT_EQ(expected_size, kafka_key_set.size());
  alter_options.action = Action::kDisableDuplication;
  ASSERT_OK(AlterTable(alter_options));
}

TEST_F(DuplicationWithKerberosKafkaITest, DuplicationWithInvalidKerberosOptions) {
  string kTableName = "DuplicationWithInvalidKerberosOptions";
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

  int insert_count = 1;
  int expected_size = insert_count;
  ASSERT_OK(InsertRows(0, insert_count));

  constexpr const int kTimeoutS = 10;
  kudu::KerberosOptions kerberos_options;
  {
    // 1. Invalid protocol
    kerberos_options.set_security_protocol(kudu::KerberosOptions::PLAINTEXT);
    kerberos_options.set_service_name("kafka");
    kerberos_options.set_keytab(kafka_keytab_file_);
    kerberos_options.set_principal(kafka_principal_);
    alter_options.info.kerberos_options = &kerberos_options;

    set<int32_t> kafka_key_set;
    string kafka_consumer_group = "AlterTableWithDuplicationAndTestDuplication_1";
    alter_options.action = Action::kEnableDuplication;
    ASSERT_OK(AlterTable(alter_options));
    std::thread t(std::bind(&DuplicationWithKerberosKafkaITest::Consume,
                             this,
                             expected_size,
                             kafka_consumer_group,
                             kSmallest,
                             kTimeoutS,
                             &kafka_key_set));
    ASSERT_OK(InsertRows(expected_size + 1, expected_size + 1 + insert_count));
    expected_size += insert_count;
    t.join();
    ASSERT_EQ(0, kafka_key_set.size());
    SleepFor(MonoDelta::FromSeconds(2));
    alter_options.action = Action::kDisableDuplication;
    ASSERT_OK(AlterTable(alter_options));
  }

  {
    // 2. Invalid service name
    set<int32_t> kafka_key_set;
    string kafka_consumer_group = "AlterTableWithDuplicationAndTestDuplication_2";
    kerberos_options.set_security_protocol(kudu::KerberosOptions::SASL_PLAINTEXT);
    kerberos_options.set_service_name("kafka_service");
    alter_options.info.kerberos_options = &kerberos_options;

    alter_options.action = Action::kEnableDuplication;
    ASSERT_OK(AlterTable(alter_options));
    std::thread t(std::bind(&DuplicationWithKerberosKafkaITest::Consume,
                             this,
                             expected_size,
                             kafka_consumer_group,
                             kSmallest,
                             kTimeoutS,
                             &kafka_key_set));
    ASSERT_OK(InsertRows(expected_size + 1, expected_size + 1 + insert_count));
    expected_size += insert_count;
    t.join();
    ASSERT_EQ(0, kafka_key_set.size());

    alter_options.action = Action::kDisableDuplication;
    SleepFor(MonoDelta::FromSeconds(2));
    ASSERT_OK(AlterTable(alter_options));
  }

  {
    // TODO(duyuqi)
    // Need add another case to test invalid keytab or principal.
    // 3. switch to normal case
    SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_switch_to_leader_checker_internal_ms));
    set<int32_t> kafka_key_set;
    string kafka_consumer_group = "AlterTableWithDuplicationAndTestDuplication_4";
    kerberos_options.set_service_name("kafka");
    alter_options.info.kerberos_options = &kerberos_options;

    alter_options.action = Action::kEnableDuplication;
    ASSERT_OK(AlterTable(alter_options));
    SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_switch_to_leader_checker_internal_ms));
    std::thread t(std::bind(&DuplicationWithKerberosKafkaITest::Consume,
                             this,
                             expected_size,
                             kafka_consumer_group,
                             kSmallest,
                             kTimeoutS,
                             &kafka_key_set));
    ASSERT_OK(InsertRows(expected_size + 1, expected_size + 1 + insert_count));
    expected_size += insert_count;
    t.join();

    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(options.table_name, &table));
    set<int32_t> kudu_key_set;
    ScanRows(table, &kudu_key_set);
    ASSERT_EQ(expected_size, kudu_key_set.size());
    ASSERT_EQ(kudu_key_set.size(), kafka_key_set.size());

    alter_options.action = Action::kDisableDuplication;
    ASSERT_OK(AlterTable(alter_options));
  }
}

}  // namespace kudu
