#include "kudu/duplicator/duplication_replay.h"
#include <llvm/IR/Intrinsics.h>
#include <string>

#include "kudu/common/scan_spec.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace duplicator {

using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftPeerPB;
using kudu::log::Log;
using kudu::tablet::TabletReplica;
using std::unique_ptr;

class DuplicationReplayWalTest : public tablet::TabletReplicaTestBase {
 public:
  DuplicationReplayWalTest()
      : tablet::TabletReplicaTestBase(GetSimpleTestSchema()),
        wal_replay_(nullptr),
        insert_counter_(0) {}

  void SetUp() override {
    // FLAGS_v = 1;
    TabletReplicaTestBase::SetUp();
    CHECK_OK(ThreadPoolBuilder("dupli").Build(&duplicate_pool_));
    CHECK_OK(ThreadPoolBuilder("replay").Build(&replay_pool_));
    tablet_replica_->TEST_set_duplication_and_replay_pool(duplicate_pool_.get(),
                                                          replay_pool_.get());
    consensus::ConsensusBootstrapInfo info;
    StartReplicaAndWaitUntilLeader(info);
    VLOG(0) << "Setup finish";
  }

  void TearDown() override { kudu::tablet::TabletReplicaTestBase::TearDown(); }

 protected:
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

  void Scan() {
    Schema projection = schema_.CopyWithoutColumnIds();
    unique_ptr<RowwiseIterator> iter;
    tablet()->NewRowIterator(projection, &iter);

    // ScanSpec spec;
    // unique_ptr<ScanSpec> orig_spec(new ScanSpec(spec));
    Status s = iter->Init(nullptr);
    int64_t rows_scanned = 0;
    RowBlockMemory mem(32 * 1024);
    RowBlock block(&iter->schema(), 256, &mem);
    VLOG(0) << "iter: " << iter->ToString() << ", " << tablet()->MemRowSetEmpty()
            << ", empty: " << tablet()->MemRowSetEmpty();
    while (iter->HasNext()) {
      CHECK_OK(iter->NextBlock(&block));
      if (PREDICT_TRUE(block.nrows() > 0)) {
        rows_scanned += block.nrows();
      }
    }
    VLOG(0) << "scan all rows size: " << rows_scanned;
  }

  Status GenWriteRequestPB(const Schema& schema, tserver::WriteRequestPB* write_req) {
    write_req->set_tablet_id(tablet()->tablet_id());
    RETURN_NOT_OK(SchemaToPB(schema, write_req->mutable_schema()));

    std::string temp("hello");
    KuduPartialRow row(&schema);
    RETURN_NOT_OK(row.SetInt32("key", insert_counter_));
    RETURN_NOT_OK(row.SetInt32("int_val", insert_counter_));
    RETURN_NOT_OK(row.SetString("string_val", temp + std::to_string(insert_counter_)));
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
    dup_info.set_name("hello");
    dup_info.set_type(consensus::DownstreamType::KAFKA);
    peer->mutable_dup_info()->CopyFrom(dup_info);

    std::shared_ptr<consensus::RaftConsensus> consensus = tablet_replica_->shared_consensus();
    boost::optional<tserver::TabletServerErrorPB::Code> error_code;
    Status s = consensus->ChangeConfig(
        req,
        [&req, &resp, this](const Status& s) { HandleResponse(req, resp, nullptr, s); },
        &error_code);
    ASSERT_TRUE(error_code == boost::none);
    ASSERT_OK(s);
  }
  void HandleResponse(consensus::ChangeConfigRequestPB& /* req */,
                      consensus::ChangeConfigResponsePB& /* resp */,
                      void* empty,
                      const Status& s) {
    VLOG(0) << "ChangeConfigResponsePB status: " << s.ToString();
  }

  WalReplay* wal_replay_;
  unique_ptr<ThreadPool> duplicate_pool_;
  unique_ptr<ThreadPool> replay_pool_;
  int64_t insert_counter_;
};

TEST_F(DuplicationReplayWalTest, RestartAndCheck) {
  for (int i = 0; i < 8; i++) {
    Insert();
  }
  Scan();
  ASSERT_OK(RestartReplica(false));
  for (int i = 0; i < 256; i++) {
    Insert();
  }
  Scan();
}

TEST_F(DuplicationReplayWalTest, PrepareAndReplay) {
  Insert();
  Scan();
  AddDuplicator();
  ASSERT_TRUE(tablet_replica_->ShouldDuplication());
  tablet_replica_->StartDuplicator();
  wal_replay_ = tablet_replica_->duplicator()->wal_replay();
  VLOG(0) << "committed: " << tablet_replica()->last_committed_opid().DebugString()
          << ", confirmed: " << tablet_replica()->last_confirmed_opid();
  for (int i = 0; i < 127; i++) {
    Insert();
  }
  Scan();
  VLOG(0) << "committed: " << tablet_replica()->last_committed_opid().DebugString()
          << ", confirmed: " << tablet_replica()->last_confirmed_opid().DebugString();

  ASSERT_OK(RestartReplica(false));
  Scan();
  tablet_replica_->TEST_set_duplication_and_replay_pool(duplicate_pool_.get(), replay_pool_.get());
  tablet_replica_->StartDuplicator();
  Scan();
}

}  // namespace duplicator
}  // namespace kudu
