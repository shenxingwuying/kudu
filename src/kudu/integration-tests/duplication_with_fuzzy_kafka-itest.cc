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
#include <cstdlib>
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
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/single_broker_kafka.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

DECLARE_int64(duplicator_max_queue_size);
DECLARE_int32(kafka_connector_flush_timeout_ms);
DECLARE_uint32(switch_to_leader_checker_internal_ms);

using std::unique_ptr;
using std::set;
using std::string;
using std::vector;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::kafka::RawKuduRecord;
using kudu::rpc::RpcController;
using kudu::tserver::TabletServerErrorPB;

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

// Simple base utility class to provide an external mini cluster with common
// setup routines useful for integration tests. And start kafka service to test
// data duplication.
class DuplicationFuzzyITest : public ExternalMiniClusterITestBase {
 public:
  // default topic.
  const string kTopicName = "kudu_profile_record_stream";
  // default kafka uri(brokers).
  const int kOffsetPort = 10;
  const string kBrokers =
      Substitute("localhost:$0", duplication::kafka::kKafkaBasePort + kOffsetPort);

  const string kSmallest = "smallest";
  const string kLargest = "largest";

  DuplicationFuzzyITest()
      : kafka_(kOffsetPort), schema_(client::KuduSchema::FromSchema(GetSimpleTestSchema())) {
    cluster_opts_.num_tablet_servers = 4;

    cluster_opts_.extra_tserver_flags.emplace_back("--enable_wait_producer_flush_for_testing=true");
    cluster_opts_.extra_tserver_flags.emplace_back("--v=0");
    cluster_opts_.extra_tserver_flags.emplace_back("--log_segment_size_mb=1");
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
      if (!status.ok()) {
        string message = Substitute("InsertRows failed, retry_index: $0", ++retry_index);
        if (retry_index > 3) {
          LOG(ERROR) << Substitute("$0, do not retry, ignore it due to more than 3 times",
                                   message);
          break;
        }
        LOG(ERROR) << Substitute(message);
      }
    }
    MonoDelta delta = MonoTime::Now() - now;
    LOG(INFO) << "WriteRows cost: " << delta.ToString() << ", retry_index: " << retry_index
              << ", result: " << status.ok();
  }

  Status UpsertRows(int batch = 1024) const {
    client::sp::shared_ptr<client::KuduSession> session = table_->client()->NewSession();
    KUDU_RETURN_NOT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_BACKGROUND));
    session->SetTimeoutMillis(2000);
    Random r(SeedRandom());
    const string kConstString(1024, 'a');
    for (int i = 0; i < 4 * batch; i++) {
      client::KuduUpsert* upsert = table_->NewUpsert();
      KuduPartialRow* row = upsert->mutable_row();
      int index_int = r.Uniform(batch / 4);
      KUDU_CHECK_OK(row->SetInt32("key", index_int));
      KUDU_CHECK_OK(row->SetInt32("int_val", index_int * 2));
      KUDU_CHECK_OK(row->SetString("string_val", kConstString));
      KUDU_CHECK_OK(session->Apply(upsert));
    }
    RETURN_NOT_OK(session->Flush());
    // Close the session.
    return session->Close();
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

  enum class CheckFlag {
    kInsert,
    kUpsert
  };

  void Consume(int expected_size,
               const string& group_name,
               const string& strategy,
               int32_t timeout_s,
               CheckFlag flag,
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
          int32_t str_value_int = -2;
          for (int i = 0; i < record.properties_size(); i++) {
            const kafka::RawProperty& property = record.properties(i);
            if ("key" == property.name()) {
              ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &key));
              key_set->insert(key);
            }
            if ("int_val" == property.name()) {
              ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &value_int));
            }
            if (flag == CheckFlag::kInsert && "string_val" == property.name()) {
              ASSERT_TRUE(SimpleAtoi(property.value().c_str(), &str_value_int));
            }
          }
          if (flag == CheckFlag::kInsert) {
            ASSERT_EQ(key, value_int);
            ASSERT_EQ(2 * key, str_value_int);
          } else {
            ASSERT_EQ(2 * key, value_int);
          }
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
                       set<int32_t>* key_set, CheckFlag flag = CheckFlag::kInsert) {
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
        if (flag == CheckFlag::kInsert) {
          ASSERT_EQ(key, int_val);
          int32_t string_val_int;
          ASSERT_TRUE(SimpleAtoi(slice_val.ToString().c_str(), &string_val_int));
          ASSERT_EQ(string_val_int, 2 * key);
        } else {
          ASSERT_EQ(2 * key, int_val);
        }
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

  static void StatusCB(void* /* unused */, const Status& status) {
    KUDU_LOG(INFO) << "Asynchronous flush finished with status: " << status.ToString();
  }

  Status InsertRows(int from, int to, int* next = nullptr) const {
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

  Status LeaderStepDown(const string& table_name, int count) {
    client::sp::shared_ptr<client::KuduTable> client_table;

    RETURN_NOT_OK(client_->OpenTable(table_name, &client_table));
    vector<client::KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    client::KuduScanTokenBuilder builder(client_table.get());
    RETURN_NOT_OK(builder.Build(&tokens));
    string tablet_id = tokens[0]->tablet().id();

    int failed_count = 0;
    for (int i = 0; i < count; i++) {
      string leader_uuid;
      HostPort leader_hp;
      bool no_leader = true;
      client::KuduTablet* tablet_raw = nullptr;
      RETURN_NOT_OK(client_->GetTablet(tablet_id, &tablet_raw));
      unique_ptr<client::KuduTablet> tablet(tablet_raw);

      for (const auto* r : tablet->replicas()) {
        if (r->is_leader()) {
          no_leader = false;
          leader_uuid = r->ts().uuid();
          leader_hp.set_host(r->ts().hostname());
          leader_hp.set_port(r->ts().port());
          break;
        }
      }
      if (no_leader) {
        i--;
        continue;
      }

      unique_ptr<consensus::ConsensusServiceProxy> proxy;
      RETURN_NOT_OK(tools::BuildProxy(leader_hp.host(), leader_hp.port(), &proxy));

      LeaderStepDownRequestPB req;
      req.set_dest_uuid(leader_uuid);
      req.set_tablet_id(tablet_id);
      req.set_mode(consensus::ABRUPT);
      RpcController rpc;
      rpc.set_timeout(MonoDelta::FromSeconds(1));
      consensus::LeaderStepDownResponsePB resp;
      RETURN_NOT_OK(proxy->LeaderStepDown(req, &resp, &rpc));
      if (resp.has_error()) {
        if (resp.error().code() == TabletServerErrorPB::NOT_THE_LEADER) {
          i--;
          SleepFor(MonoDelta::FromMilliseconds(500));
          continue;
        }
        failed_count++;
        LOG(ERROR) << Substitute("resp: $0 status: $1",
                                 resp.ShortDebugString(),
                                 StatusFromPB(resp.error().status()).ToString());
        continue;
      }
      SleepFor(MonoDelta::FromMilliseconds(500));
    }
    if (failed_count > static_cast<int32_t>(0.9 * count)) {
      return Status::NetworkError(
          Substitute("leader down failed rate: $0/$1", failed_count, count));
    }
    return Status::OK();
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

TEST_F(DuplicationFuzzyITest, FuzzyTestFrequentKafkaClusterRestart) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  {
    // TODO(duyuqi)
    LOG(INFO) << "MockExecute, DuplicationFuzzyITest::FuzzyTestFrequentKafkaClusterRestart, "
                "this test is very slow, so I plan move to other code path. Skip this test.";
    GTEST_SKIP();
  }
  // Below code is not executed.
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  dup_info.kerberos_options = nullptr;
  options.dup_info = dup_info;
  options.partition_num = 9;
  options.replication_refactor = 3;
  options.table_name = "FuzzyTestFrequentKafkaClusterRestart";
  ASSERT_OK(CreateTable(options));

  int insert_count = 1000000;
  int expected_size = insert_count;
  set<int32_t> kafka_key_set;
  constexpr const int session_timeout_s = 20;

  std::unique_ptr<ThreadPool> write_pool;
  int task_num = 5;
  ThreadPoolBuilder("write_pool")
      .set_min_threads(task_num)
      .set_max_threads(task_num)
      .Build(&write_pool);
  int step = insert_count / task_num;
  int from = 0;
  int to = step;
  for (int i = 0; i < task_num; i++) {
    Status status = write_pool->Submit([this, from, to]() {
      WriteRows(from, to);
    });
    ASSERT_OK(status);
    from += step;
    to += step;
  }

  constexpr const int kTimesOfRestartKafkaCluster = 10;
  CountDownLatch stop_fault_injection_latch(1);
  CountDownLatch fault_injection_latch(kTimesOfRestartKafkaCluster);
  std::thread stop_fault_injection([&stop_fault_injection_latch, &fault_injection_latch]() {
    while (!fault_injection_latch.WaitFor(MonoDelta::FromSeconds(1))) {
    }
    stop_fault_injection_latch.CountDown();
  });
  while (!stop_fault_injection_latch.WaitFor(MonoDelta::FromSeconds(1))) {
    // Restart kafka cluster continuously, run 'kCountOfRestartKafkaCluster' times.
    int count = 0;
    while (kafka_.Alive()) {
      count++;
      kafka_.StopBrokers();
    }
    SleepFor(MonoDelta::FromSeconds(2));
    count = 0;
    while (!kafka_.Alive()) {
      count++;
      kafka_.StartBrokers();
    }
    SleepFor(MonoDelta::FromSeconds(3));
    fault_injection_latch.CountDown();
  }

  stop_fault_injection.join();
  write_pool->Wait();
  kafka_key_set.clear();
  Consume(expected_size,
          options.table_name + "first",
          kSmallest,
          session_timeout_s,
          CheckFlag::kInsert,
          &kafka_key_set);

  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(options.table_name, &table));
  set<int32_t> kudu_key_set;
  ScanRows(table, &kudu_key_set);
  if (kafka_key_set.size() == kudu_key_set.size()) {
    LOG(INFO) << Substitute("first check result, success, kudu size: $0, kafka size: $1",
                            kudu_key_set.size(),
                            kafka_key_set.size());
    return;
  }
  LOG(INFO) << Substitute(
      "check failed, kudu size: $0, kafka size: $1", kudu_key_set.size(), kafka_key_set.size());
  bool retry_start_kafka = true;
  for (int i = 0; i < 5; i++) {
    auto old_kafka_key_size = kafka_key_set.size();
    kafka_key_set.clear();
    string group = options.table_name;
    group.append(std::to_string(i));
    SleepFor(MonoDelta::FromSeconds(2));
    Consume(expected_size, group, kSmallest, session_timeout_s, CheckFlag::kInsert, &kafka_key_set);
    if (kafka_key_set.size() == kudu_key_set.size()) {
      LOG(INFO) << Substitute("others check result, success");
      break;
    }
    if (kafka_key_set.empty() ||
        (old_kafka_key_size == kafka_key_set.size() && retry_start_kafka)) {
      retry_start_kafka = false;
      LOG(INFO) << "kafka size: empty, keep a snapshot of ps auxwf, maybe kafka is down, try "
                   "StartKafka, retry index: " << i;
      system("ps auxwf");
      kafka_.StartBrokers();
      SleepFor(MonoDelta::FromSeconds(10));
      string rpc_addrs;
      for (int i = 0; i < cluster_->master_rpc_addrs().size(); i++) {
        rpc_addrs.append(cluster_->master_rpc_addrs()[i].ToString());
        if (i != (cluster_->master_rpc_addrs().size() - 1)) {
          rpc_addrs.append(",");
        }
      }
      string exe_file;
      CHECK_OK(Env::Default()->GetExecutablePath(&exe_file));
      string ksck_cmd =
          Substitute("$0 cluster ksck $1", JoinPathSegments(DirName(exe_file), "kudu"), rpc_addrs);
      LOG(INFO) << "Keep ksck result, check whether kudu is ok? ksck_cmd: " << ksck_cmd;

      system(ksck_cmd.c_str());
      continue;
    }
    LOG(INFO) << Substitute(
        "check failed, kudu size: $0, kafka size: $1", kudu_key_set.size(), kafka_key_set.size());
    if (old_kafka_key_size == kafka_key_set.size()) {
      LOG(INFO) << Substitute("kafka size no changes between two consumes", kafka_key_set.size());
      break;
    }
  }

  for (const auto& key : kudu_key_set) {
    if (kafka_key_set.count(key) <= 0) {
      VLOG(0) << "kudu minus kafka key: " << key;
    }
  }
  for (const auto& key : kafka_key_set) {
    if (kudu_key_set.count(key) <= 0) {
      VLOG(0) << "impossible kafka minus kudu key: " << key;
    }
  }

  ASSERT_EQ(kudu_key_set.size(), kafka_key_set.size());
}

// It is a test for breaking row lock deadlock. Using kudu client write a lot of records
// quickly, in these records there are many duplicated keys in these keys. We send a leader
// transferring continuously to cause some row lock or wait lock temporarily.
// Make sure this environment eventually writes requests is normal and does not get stuck.
TEST_F(DuplicationFuzzyITest, StressWithDuplicatedKeysToTestSolveRowLockDeadLockTest) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  CreateTableOptions options;
  client::DuplicationInfo dup_info;
  dup_info.name = kTopicName;
  dup_info.type = client::DuplicationDownstream::KAFKA;
  dup_info.uri = kBrokers;
  options.dup_info = dup_info;
  options.partition_num = 1;
  options.replication_refactor = 3;
  options.table_name = "StressWithDuplicatedKeys";
  ASSERT_OK(CreateTable(options));

  std::thread async_write_thread([this]() {
    // Create a thread pool with 10 threads for kudu writes and submit lots of writes tasks.
    std::unique_ptr<ThreadPool> write_pool;
    int task_num = 4;
    ThreadPoolBuilder("write_pool")
        .set_min_threads(task_num)
        .set_max_threads(task_num)
        .Build(&write_pool);
    int count = 500;
    while (count-- >= 0) {
      WARN_NOT_OK(write_pool->Submit([this]() {
        Status s = UpsertRows(64);
        LOG_IF(WARNING, !s.ok()) << Substitute("UpsertRows error $0", s.ToString());
      }), "submit an upsert task failed");
    }
    write_pool->Wait();
  });

  constexpr const int kTimesOfLeaderStepDown = 20;
  ASSERT_OK(LeaderStepDown(options.table_name, kTimesOfLeaderStepDown));
  async_write_thread.join();
  if (!UpsertRows(64).ok()) {
    SleepFor(MonoDelta::FromSeconds(3));
    if (!UpsertRows(64).ok()) {
      SleepFor(MonoDelta::FromSeconds(6));
      if (!UpsertRows(64).ok()) {
        SleepFor(MonoDelta::FromSeconds(12));
      }
    }
  }
  // Use this statement to check kudu is not stuck due to row lock.
  CHECK_OK(UpsertRows(64));

  int expected_size = 0;
  constexpr const int session_timeout_s = 20;
  int last_size = -1;
  set<int32_t> kafka_key_set;
  for (int i = 0; i < 3; i++) {
    kafka_key_set.clear();
    Consume(expected_size,
            options.table_name + std::to_string(i),
            kSmallest,
            session_timeout_s,
            CheckFlag::kUpsert,
            &kafka_key_set);
    int current = kafka_key_set.size();
    VLOG(0) << Substitute("last_size: $0, current: $1", last_size, current);
    if (last_size == current && current != 0) {
      break;
    }
    last_size = current;
  }

  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(options.table_name, &table));
  set<int32_t> kudu_key_set;
  ScanRows(table, &kudu_key_set, CheckFlag::kUpsert);
  ASSERT_EQ(kudu_key_set.size(), kafka_key_set.size());
}


}  // namespace kudu
