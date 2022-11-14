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
// The file is define a Duplicator interface api,
// if user need to duplicate data to another storage,
// user should implement the interface just like kafka below.

#include "kudu/duplicator/duplicator.h"

#include <atomic>
#include <deque>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/duplicator/connector_manager.h"
#include "kudu/duplicator/duplication_replay.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

using strings::Substitute;
using std::unique_ptr;
using std::vector;

DEFINE_int64(duplicator_max_queue_size, 1024,
            "the duplicator's max queue size, if excess the limit, duplication would downgrade "
            "from realtime mode to wal mode, duplication progress would be slow.");
DEFINE_int32(queue_drain_timeout_ms, 5000,
             "if queue no rows, wait queue_drain_timeout_ms ms at most ");

using Tablet = kudu::tablet::Tablet;
using DuplicationMode = kudu::tablet::Tablet::DuplicationMode;

namespace kudu {
namespace duplicator {

Duplicator::Duplicator(ThreadPool* duplicate_pool,
                       tablet::TabletReplica* tablet_replica)
    : duplicate_pool_(duplicate_pool),
      tablet_replica_(tablet_replica),
      last_confirmed_opid_(consensus::MinimumOpId()),
      stopped_(true),
      queue_(FLAGS_duplicator_max_queue_size),
      realtime_tmp_queue_(FLAGS_duplicator_max_queue_size),
      need_replay_again_(false),
      log_replayer_(new duplicator::LogReplayer(tablet_replica_)),
      replay_task_id_(-1) {
  connector_manager_ = tablet_replica->connector_manager();
  duplication_mode_.store(DuplicationMode::INIT);
}

Duplicator::~Duplicator() {}

Status Duplicator::Init(const ConnectorOptions& options) {
  options_ = options;
  connector_ = connector_manager_->GetOrNewConnector(options);
  // TODO(duyuqi) metric_entity for duplicate_pool_
  duplicate_pool_token_ = duplicate_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);

  log_replayer_->Init();
  CHECK(DuplicationMode::INIT == duplication_mode_.load());
  duplication_mode_.store(DuplicationMode::WAL_DUPLICATION);
  Status status = log_replayer_->SeekStartPoint();
  if (status.ok()) {
    LOG_WITH_PREFIX(INFO) << Substitute("find wal start point $0, status ",
                                        log_replayer_->start_point().ShortDebugString());
  } else {
    LOG_WITH_PREFIX(WARNING) << Substitute("not find start point, replay all wals, status: $0",
                                           status.ToString());
  }
  log_replayer_->TriggerReplayTask(++replay_task_id_);
  stopped_ = false;
  return Status::OK();
}

Status Duplicator::Shutdown() {
  stopped_ = true;
  if (duplicate_pool_token_) {
    duplicate_pool_token_->Shutdown();
    duplicate_pool_token_.reset();
  }
  log_replayer_->Shutdown();
  connector_ = nullptr;
  return Status::OK();
}

Status Duplicator::Duplicate(tablet::WriteOpState* write_op_state,
                             tablet::Tablet::DuplicationMode expect_mode) {
  if (stopped_) {
    return Status::IllegalState("duplicator is stopped");
  }
  std::unique_ptr<DuplicateMsg> msg = std::make_unique<DuplicateMsg>(
      write_op_state, tablet_replica_->tablet_metadata()->table_name());

  RETURN_NOT_OK_LOG(msg->ParseKafkaRecord(), ERROR, "parse kafka record failed");

  // TODO(duyuqi)
  // adding metrics for the 'duplication_mode_'.
  switch (duplication_mode_.load()) {
    case DuplicationMode::INIT: {
      LOG_WITH_PREFIX(FATAL) << "Unbelievable, a message is duplicating in INIT mode.";
      break;
    }
    case DuplicationMode::WAL_DUPLICATION: {
      RETURN_NOT_OK(WorkAtWalReplay(std::move(msg), expect_mode));
      break;
    }
    case DuplicationMode::WAL_DUPLICATION_FINISH: {
      // TODO(duyuqi)
      // Remove the state: DuplicationMode::WAL_DUPLICATION_FINISH ?
      RETURN_NOT_OK(WorkAtWalReplayFinished(std::move(msg), expect_mode));
      break;
    }
    case DuplicationMode::REALTIME_DUPLICATION: {
      RETURN_NOT_OK(WorkAtRealtime(std::move(msg), expect_mode));
      break;
    }
    default: {
      LOG_WITH_PREFIX(FATAL) << "unknown duplication mode";
    }
  }
  RETURN_NOT_OK_LOG(duplicate_pool_token_->Submit([this]() { this->Apply(); }),
                    WARNING,
                    Substitute("$0 duplicator submit apply task failed", LogPrefix()));
  return Status::OK();
}

void Duplicator::Apply() {
  vector<unique_ptr<DuplicateMsg>> msgs;
  bool should_merge_msgs = false;
  if (duplication_mode_.load() == DuplicationMode::REALTIME_DUPLICATION) {
    // If current mode is realtime and realtime_tmp_queue_ is not empty.
    // the realtime_tmp_queue should write to destination system, then
    // current queue_ write to destination.
    std::unique_lock l(realtime_tmp_lock_);
    if (!realtime_tmp_queue_.empty()) {
      Status s = realtime_tmp_queue_.BlockingDrainTo(
          &msgs, MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_queue_drain_timeout_ms));
      if (PREDICT_FALSE(s.IsAborted() || s.IsTimedOut())) {
        return;
      }
      should_merge_msgs = true;
    }
  }

  if (should_merge_msgs && !queue_.empty()) {
    std::vector<std::unique_ptr<DuplicateMsg>> messages;
    Status s = queue_.BlockingDrainTo(
        &messages, MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_queue_drain_timeout_ms));
    if (PREDICT_FALSE(s.IsAborted() || s.IsTimedOut())) {
      return;
    }
    msgs.reserve(msgs.size() + messages.size());
    std::move(messages.begin(), messages.end(), std::back_inserter(msgs));
  } else {
    Status s = queue_.BlockingDrainTo(
        &msgs, MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_queue_drain_timeout_ms));
    if (PREDICT_FALSE(s.IsAborted() || s.IsTimedOut())) {
      return;
    }
  }

  if (msgs.empty()) {
    return;
  }

  Status s = Status::OK();
  // TODO(duyuqi)
  // 1. Try to avoid duplicated messages because of partial failed.
  // 2. limit write batch size? produce some data and flush?
  while (!(s = connector_->WriteBatch(options_.name, msgs)).ok()) {
    KLOG_EVERY_N(WARNING, 100)
        << LogPrefix()
        << Substitute("WriteBatch destination storage system error, retry it, status $0",
                      s.ToString());
    // @TODO(duyuqi). We'd better add some backoff time.
    SleepFor(MonoDelta::FromMilliseconds(20));
  }
  auto rit = msgs.rbegin();
  {
    std::lock_guard<std::mutex> l(mutex_);
    last_confirmed_opid_ = (*rit)->op_id();
  }
}

Status Duplicator::WorkAtWalReplay(unique_ptr<DuplicateMsg> msg,
                                   tablet::Tablet::DuplicationMode expect_mode) {
  CHECK(expect_mode == DuplicationMode::WAL_DUPLICATION ||
        expect_mode == DuplicationMode::REALTIME_DUPLICATION);
  DuplicationMode wal_mode = DuplicationMode::WAL_DUPLICATION;
  if (log_replayer_->is_finished(replay_task_id_) &&
      PREDICT_TRUE(
          duplication_mode_.compare_exchange_strong(wal_mode,
                                                    DuplicationMode::WAL_DUPLICATION_FINISH,
                                                    std::memory_order_seq_cst,
                                                    std::memory_order_seq_cst))) {
    LOG_WITH_PREFIX(INFO) << Substitute(
        "switch mode from $0 to $1",
        Tablet::DuplicationMode_Name(wal_mode),
        Tablet::DuplicationMode_Name(DuplicationMode::WAL_DUPLICATION_FINISH));
    return WorkAtWalReplayFinished(std::move(msg), expect_mode);
  }
  if (expect_mode == DuplicationMode::WAL_DUPLICATION) {
    // Avoid using BlockingPut, because it will cause apply pool stuck indrection.
    switch (queue_.Put(std::move(msg))) {
      case QueueStatus::QUEUE_SUCCESS:
        return Status::OK();
      case QueueStatus::QUEUE_FULL:
        return Status::Incomplete("queue full, put fail, please retry put again");
      case QueueStatus::QUEUE_SHUTDOWN:
        return Status::IllegalState("queue shutdown when replay wals");
      default:
        LOG_WITH_PREFIX(FATAL) << "impossble state";
    }
    __builtin_unreachable();
  }
  CHECK(DuplicationMode::REALTIME_DUPLICATION == expect_mode);
  // If expect_mode is realtime mode.
  std::unique_lock l(realtime_tmp_lock_);
  if (realtime_tmp_queue_.empty()) {
    realtime_tmp_queue_top_op_id_ = msg->op_id();
  }
  CHECK(realtime_tmp_queue_top_op_id_.IsInitialized());
  realtime_tmp_queue_.Put(std::move(msg));
  if (realtime_tmp_queue_.size() > FLAGS_duplicator_max_queue_size / 2) {
    realtime_tmp_queue_.Clear();
    need_replay_again_ = true;
  }

  return Status::OK();
}

void Duplicator::ReplayWals() {
  queue_.Clear();
  consensus::OpId restart_opid = last_confirmed_opid();
  log_replayer_->SetStartPoint(restart_opid);
  log_replayer_->TriggerReplayTask(++replay_task_id_);
  need_replay_again_ = false;
}

Status Duplicator::WorkAtWalReplayFinished(unique_ptr<DuplicateMsg> msg,
                                           tablet::Tablet::DuplicationMode expect_mode) {
  CHECK(expect_mode == DuplicationMode::WAL_DUPLICATION ||
        expect_mode == DuplicationMode::REALTIME_DUPLICATION);
  if (expect_mode == DuplicationMode::WAL_DUPLICATION) {
    LOG_WITH_PREFIX(INFO) << Substitute(
        "Ignore the msg which expected $0, now in $1",
        Tablet::DuplicationMode_Name(expect_mode),
        Tablet::DuplicationMode_Name(DuplicationMode::WAL_DUPLICATION_FINISH));
    return Status::OK();
  }

  DuplicationMode wal_end_mode = DuplicationMode::WAL_DUPLICATION_FINISH;
  if (need_replay_again_) {
    if (duplication_mode_.compare_exchange_strong(wal_end_mode,
                                                  DuplicationMode::WAL_DUPLICATION,
                                                  std::memory_order_seq_cst,
                                                  std::memory_order_seq_cst)) {
      ReplayWals();
    }
    // If switch fail, keep at WAL_DUPLICATION_FINISH.
    return Status::OK();
  }

  consensus::OpId top_op_id = msg->op_id();
  {
    std::unique_lock l(realtime_tmp_lock_);
    if (realtime_tmp_queue_.empty()) {
      realtime_tmp_queue_top_op_id_ = msg->op_id();
    }
    top_op_id = realtime_tmp_queue_top_op_id_;
    realtime_tmp_queue_.Put(std::move(msg));
  }

  if (top_op_id.index() <= (last_confirmed_opid_.index() + 1)) {
    if (duplication_mode_.compare_exchange_strong(wal_end_mode,
                                                  DuplicationMode::REALTIME_DUPLICATION,
                                                  std::memory_order_seq_cst,
                                                  std::memory_order_seq_cst)) {
      LOG_WITH_PREFIX(INFO) << Substitute(
          "top_op_id $0, switch mode from $1 to $2",
          top_op_id.ShortDebugString(),
          Tablet::DuplicationMode_Name(wal_end_mode),
          Tablet::DuplicationMode_Name(DuplicationMode::REALTIME_DUPLICATION));
    }
    // If switch fail, keep at WAL_DUPLICATION_FINISH.
    return Status::OK();
  }
  // TODO(duyuqi)
  // A case which is normal, but now we need view it a data absent to make sure
  // data safety.
  // The normal case is caused by AlterSchema, Noop, ChangeConfig, the ops is not
  // duplicated, so the confirmed_opid is not strictly ordered.
  // This method can avoid data loss, but it's inefficient.
  //
  // Alternative method is duplicating all ops.
  // I plan to refact the Noop, ChangeConfig, extract them from consensus module to Raft
  // OP structure, and implement 'Duplicate()' to make confirmed_opid strictly ordered.
  if (duplication_mode_.compare_exchange_strong(wal_end_mode,
                                                DuplicationMode::WAL_DUPLICATION,
                                                std::memory_order_seq_cst,
                                                std::memory_order_seq_cst)) {
    LOG_WITH_PREFIX(INFO) << strings::Substitute(
        "op_id_tmp $0, switch mode from $1 to $2",
        top_op_id.ShortDebugString(),
        Tablet::DuplicationMode_Name(wal_end_mode),
        Tablet::DuplicationMode_Name(DuplicationMode::WAL_DUPLICATION));
    ReplayWals();
  }
  return Status::OK();
}

Status Duplicator::WorkAtRealtime(unique_ptr<DuplicateMsg> msg,
                                  tablet::Tablet::DuplicationMode expect_mode) {
  CHECK(expect_mode == DuplicationMode::WAL_DUPLICATION ||
        expect_mode == DuplicationMode::REALTIME_DUPLICATION);
  if (expect_mode == DuplicationMode::WAL_DUPLICATION) {
    CHECK(!msg->op_state()->row_ops().empty());
    VLOG_WITH_PREFIX(0) << Substitute(
        "ignore wal mode message, continue replaying wals, row_op[0]: $0",
        (msg->op_state()->row_ops()[0])->ToString(*msg->op_state()->schema_at_decode_time()));
    return Status::OK();
  }
  DuplicationMode realtime_mode = DuplicationMode::REALTIME_DUPLICATION;

  switch (queue_.Put(std::move(msg))) {
    case QueueStatus::QUEUE_SUCCESS:
      break;
    case QueueStatus::QUEUE_FULL: {
      LOG_WITH_PREFIX(INFO) << "duplicator queue full, will downgrade from realtime to wal_mode.";
      if (PREDICT_TRUE(duplication_mode_.compare_exchange_strong(realtime_mode,
                                                                 DuplicationMode::WAL_DUPLICATION,
                                                                 std::memory_order_seq_cst,
                                                                 std::memory_order_seq_cst))) {
        ReplayWals();
        return Status::OK();
      }
      // If switch fail, keep at REALTIME_DUPLICATION.
      break;
    }
    case QueueStatus::QUEUE_SHUTDOWN: {
      return Status::IllegalState("queue shutdown");
    }
    default:
      LOG_WITH_PREFIX(FATAL) << "Unknown status";
      __builtin_unreachable();
  }
  return Status::OK();
}

string Duplicator::LogPrefix() const {
  return Substitute("duplicator info: confirmed opid $0, tablet id $1, peer uuid $2 ",
                    last_confirmed_opid_.ShortDebugString(),
                    tablet_replica_->tablet_id(),
                    tablet_replica_->permanent_uuid());
}

}  // namespace duplicator
}  // namespace kudu
