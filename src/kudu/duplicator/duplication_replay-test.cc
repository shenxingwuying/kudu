#include <stdlib.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <cppkafka/buffer.h>
#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cppkafka/error.h>
#include <cppkafka/message.h>
#include <cppkafka/topic_partition_list.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/duplicator/connector.h"
#include "kudu/duplicator/connector_manager.h"
#include "kudu/duplicator/duplicator.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/single_broker_kafka.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

METRIC_DECLARE_gauge_uint64(ops_duplicator_lag);

namespace kudu {
namespace duplicator {
class LogReplayer;

using kudu::consensus::RaftPeerPB;
using std::unique_ptr;

static std::string kTopicName = "kudu_profile_record_stream";
static constexpr const int kOffsetPort = 5;
// Kafka Uri.
static std::string kBrokers =
    Substitute("localhost:$0", duplication::kafka::kKafkaBasePort + kOffsetPort);

class DuplicationReplayWalTest : public tablet::TabletReplicaTestBase {
 public:
  DuplicationReplayWalTest()
      : tablet::TabletReplicaTestBase(GetSimpleTestSchema()),
        log_replayer_(nullptr),
        insert_counter_(0),
        connector_manager_(new duplicator::ConnectorManager()),
        kafka_(kOffsetPort) {}

  void SetUp() override {
    kafka_.DestroyKafka();
    kafka_.InitKafka();
    TabletReplicaTestBase::SetUp();
    CHECK_OK(
        ThreadPoolBuilder("dupli").set_min_threads(1).set_max_threads(1).Build(&duplicate_pool_));
    CHECK_OK(
        ThreadPoolBuilder("replay").set_min_threads(1).set_max_threads(1).Build(&replay_pool_));
    tablet_replica_->TEST_set_duplication_and_replay_pool(duplicate_pool_.get(),
                                                          replay_pool_.get());
    tablet_replica_->TEST_set_connector_manager(connector_manager_.get());
    consensus::ConsensusBootstrapInfo info;
    StartReplicaAndWaitUntilLeader(info);
  }

  void TearDown() override {
    kudu::tablet::TabletReplicaTestBase::TearDown();
    kafka_.DestroyKafka();
  }

 protected:
  void Consumer(int expected_size, const std::string& group_name, int* key_count) {
    cppkafka::Configuration configuration = {{"metadata.broker.list", kBrokers},
                                             {"group.id", group_name},
                                             {"enable.auto.commit", false},
                                             {"auto.offset.reset", "smallest"}};

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

    std::chrono::milliseconds timeout(3000);
    int count = 0;
    int max_batch_size = 4;
    std::vector<cppkafka::Message> messages;
    int empty_count = 0;
    std::set<int32_t> key_set;
    while (true) {
      messages.clear();
      messages = consumer_->poll_batch(max_batch_size, timeout);
      if (messages.empty()) {
        if (empty_count++ >= 10) {
          break;
        }
        LOG(INFO) << "Consumer poll messages 2s, try to poll again";
        continue;
      }
      empty_count = 0;
      for (const auto& msg : messages) {
        if (msg) {
          if (msg.get_error()) {
            if (!msg.is_eof()) {
              LOG(INFO) << "Received error notification: " << msg.get_error();
            }
          } else {
            kafka::RawKuduRecord record;
            record.ParseFromString(msg.get_payload());
            int32_t key_int = -1;
            int32_t int_val = -1;
            std::string value;
            for (int i = 0; i < record.properties_size(); i++) {
              const kafka::RawProperty& property = record.properties(i);
              if ("key" == property.name()) {
                std::string str(property.value().data(), property.value().size());
                key_int = atoi(str.c_str());
                key_set.insert(key_int);
              }
              if ("int_val" == property.name()) {
                std::string str(property.value().data(), property.value().size());
                int_val = atoi(str.c_str());
              }
              if ("string_val" == property.name()) {
                value = std::string(property.value().data(), property.value().length());
              }
            }
            ASSERT_EQ(key_int, int_val);
            std::string expect_string_value("hello ");
            expect_string_value.append(std::to_string(key_int));
            ASSERT_EQ(expect_string_value, value);
            VLOG(1) << "key_int: " << key_int << ", string_val: " << value;
            consumer_->commit(msg);
            count++;
          }
        }
      }
    }

    messages.clear();
    messages = consumer_->poll_batch(max_batch_size, timeout);
    LOG(INFO) << Substitute("expect size: $0, real size: $1, count: $2, last message size: $3",
                            expected_size,
                            count + messages.size(),
                            count,
                            messages.size());

    ASSERT_GE(count, expected_size);
    ASSERT_TRUE(messages.size() >= 0);
    *key_count = static_cast<int>(key_set.size());
  }

  static void TabletReplicaStateChangedCallback(const string& tablet_id, const string& reason) {
    LOG(INFO) << "Tablet replica state changed for tablet " << tablet_id << ". Reason: " << reason;
  }

  Status Insert() {
    tserver::WriteRequestPB write;
    WARN_NOT_OK(GenWriteRequestPB(schema_.CopyWithoutColumnIds(), &write),
                "GenWriteRequestPB status not ok");
    WARN_NOT_OK(ExecuteWrite(tablet_replica_.get(), write), "write not ok");
    return Status::OK();
  }

  int Scan() {
    Schema projection = schema_.CopyWithoutColumnIds();
    unique_ptr<RowwiseIterator> iter;
    tablet_replica_->tablet()->NewRowIterator(projection, &iter);

    ScanSpec spec;
    Status s = iter->Init(&spec);
    int64_t rows_scanned = 0;
    RowBlockMemory mem(32 * 1024);
    RowBlock block(&iter->schema(), 256, &mem);
    while (iter->HasNext()) {
      CHECK_OK(iter->NextBlock(&block));
      if (PREDICT_TRUE(block.nrows() > 0)) {
        rows_scanned += block.nrows();
      }
    }
    VLOG(0) << "scan all rows size: " << rows_scanned;
    return rows_scanned;
  }

  Status GenWriteRequestPB(const Schema& schema, tserver::WriteRequestPB* write_req) {
    write_req->set_tablet_id(tablet()->tablet_id());
    RETURN_NOT_OK(SchemaToPB(schema, write_req->mutable_schema()));

    std::string temp("hello ");
    KuduPartialRow row(&schema);
    RETURN_NOT_OK(row.SetInt32("key", insert_counter_));
    RETURN_NOT_OK(row.SetInt32("int_val", insert_counter_));
    temp.append(std::to_string(insert_counter_));
    RETURN_NOT_OK(row.SetString("string_val", temp));
    insert_counter_++;

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::INSERT, row);
    return Status::OK();
  }

  void AddDuplicator() {
    consensus::ChangeConfigRequestPB req;
    consensus::ChangeConfigResponsePB resp;
    req.set_dest_uuid(tablet()->metadata()->fs_manager()->uuid());
    req.set_tablet_id(tablet()->tablet_id());
    req.set_type(consensus::ADD_PEER);
    req.set_cas_config_opid_index(tablet_replica_->consensus()->CommittedConfig().opid_index());
    RaftPeerPB* peer = req.mutable_server();
    peer->set_member_type(RaftPeerPB::DUPLICATOR);

    consensus::DuplicationInfoPB dup_info;
    dup_info.set_name(kTopicName);
    dup_info.set_type(consensus::DownstreamType::KAFKA);
    dup_info.set_uri(kBrokers);
    peer->mutable_dup_info()->CopyFrom(dup_info);

    std::shared_ptr<consensus::RaftConsensus> consensus = tablet_replica_->shared_consensus();
    boost::optional<tserver::TabletServerErrorPB::Code> error_code;
    Status s = consensus->ChangeConfig(
        req,
        [&req, &resp](const Status& s) { HandleResponse(req, resp, nullptr, s); },
        &error_code);
    ASSERT_TRUE(error_code == boost::none);
    ASSERT_OK(s);
  }
  static void HandleResponse(const consensus::ChangeConfigRequestPB& /* req */,
                             const consensus::ChangeConfigResponsePB& /* resp */,
                             void* /* empty */,
                             const Status& s) {
    VLOG(0) << "ChangeConfigResponsePB status: " << s.ToString();
  }

  LogReplayer* log_replayer_;
  unique_ptr<ThreadPool> duplicate_pool_;
  unique_ptr<ThreadPool> replay_pool_;
  int64_t insert_counter_;
  std::shared_ptr<cppkafka::Consumer> consumer_;
  std::unique_ptr<ConnectorManager> connector_manager_;
  duplication::kafka::SingleBrokerKafka kafka_;
};

TEST_F(DuplicationReplayWalTest, RestartAndCheck) {
  int batch_count = 8;
  for (int i = 0; i < batch_count; i++) {
    ASSERT_OK(Insert());
  }
  ASSERT_EQ(batch_count, Scan());
  ASSERT_OK(RestartReplica(true));
  ASSERT_EQ(batch_count, Scan());
  int another_batch_count = 16;
  for (int i = 0; i < another_batch_count; i++) {
    ASSERT_OK(Insert());
  }
  ASSERT_EQ(batch_count + another_batch_count, Scan());
}

TEST_F(DuplicationReplayWalTest, PrepareAndReplay) {
  int key_count = 0;
  int batch_count = 100;
  for (int i = 0; i < batch_count; i++) {
    ASSERT_OK(Insert());
  }
  ASSERT_EQ(batch_count, Scan());
  AddDuplicator();
  ASSERT_TRUE(tablet_replica_->consensus()->ShouldDuplication());
  tablet_replica_->StartDuplicator();
  log_replayer_ = tablet_replica_->duplicator()->log_replayer();

  int another_batch_count = 10000;
  for (int i = 0; i < another_batch_count; i++) {
    ASSERT_OK(Insert());
  }
  ASSERT_EQ(batch_count + another_batch_count, Scan());
  Consumer(batch_count + another_batch_count, "group1", &key_count);
  ASSERT_EQ(batch_count + another_batch_count, key_count);
  key_count = 0;
  ASSERT_OK(RestartReplica(true));
  ASSERT_EQ(batch_count + another_batch_count, Scan());
  tablet_replica_->TEST_set_duplication_and_replay_pool(duplicate_pool_.get(), replay_pool_.get());
  tablet_replica_->TEST_set_connector_manager(connector_manager_.get());
  tablet_replica_->StartDuplicator();
  SleepFor(MonoDelta::FromSeconds(1));

  Consumer(batch_count + another_batch_count, "group2", &key_count);
  ASSERT_EQ(batch_count + another_batch_count, key_count);
}

TEST_F(DuplicationReplayWalTest, LagMetricTest) {
  // We don't care what the function is, since the metric is already instantiated.
  auto ops_duplicator_lag = METRIC_ops_duplicator_lag.InstantiateFunctionGauge(
      tablet_replica_->tablet()->GetMetricEntity(), []() { return 0; });
  ASSERT_EQ(0, ops_duplicator_lag->value());
  int batch_count = 100;
  for (int i = 0; i < batch_count; i++) {
    ASSERT_OK(Insert());
  }
  AddDuplicator();
  ASSERT_TRUE(tablet_replica_->consensus()->ShouldDuplication());
  tablet_replica_->StartDuplicator();
  for (int i = 0; i < batch_count; i++) {
    ASSERT_OK(Insert());
  }
  ASSERT_EQ(2 * batch_count, Scan());
  SleepFor(MonoDelta::FromSeconds(1));
  ASSERT_EVENTUALLY_WITH_TIMEOUT([&]() { ASSERT_EQ(0, ops_duplicator_lag->value()); },
                                 MonoDelta::FromSeconds(60));
  kafka_.StopKafka();
  int another_batch_count = 10000;
  for (int i = 0; i < another_batch_count; i++) {
    ASSERT_OK(Insert());
  }
  ASSERT_EQ(2 * batch_count + another_batch_count, Scan());
  ASSERT_EVENTUALLY_WITH_TIMEOUT(
      [&]() {
        ASSERT_LE(another_batch_count, ops_duplicator_lag->value());
        // TODO(duyuqi) Fix the gap.
        // DUPLICATE_OP, ALTER_SCHEMA_OP, NOOP... can cause little error.
        ASSERT_GE(another_batch_count + 2, ops_duplicator_lag->value());
      },
      MonoDelta::FromSeconds(60));
  // TODO(duyuqi)
  // We should study the log below and make the case runs more quickly.
  // The SleepFor's reason.
  // [2022-11-22 17:12:55,150] ERROR Error while creating ephemeral at /brokers/ids/0, node already
  // exists and owner '72657486139949056' does not match current session '72657486139949057'
  // (kafka.zk.KafkaZkClient$CheckedEphemeral) [2022-11-22 17:12:55,153] ERROR [KafkaServer id=0]
  // Fatal error during KafkaServer startup. Prepare to shutdown (kafka.server.KafkaServer)
  // org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists
  SleepFor(MonoDelta::FromSeconds(
      2 * kudu::duplication::kafka::SingleBrokerKafka::kZookeeperSessionTimeoutS));
  kafka_.StartKafka();
  int key_count = -1;
  ASSERT_OK(Insert());
  Consumer(1 + batch_count * 2 + another_batch_count, "group3", &key_count);
  ASSERT_EVENTUALLY_WITH_TIMEOUT([&]() { ASSERT_EQ(0, ops_duplicator_lag->value()); },
                                 MonoDelta::FromSeconds(60));
}

}  // namespace duplicator
}  // namespace kudu
