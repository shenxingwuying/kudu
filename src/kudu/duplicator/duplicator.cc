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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
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

DEFINE_int32(duplication_connectors_count, 4, "connectors number for duplication");

DEFINE_int64(duplicator_max_queue_size, 1024,
            "the duplicator's max queue size, if excess the limit, duplication would downgrade "
            "from realtime mode to wal mode, duplication progress would be slow.");
DEFINE_int32(queue_drain_timeout_ms, 5000,
             "if queue no rows, wait queue_drain_timeout_ms ms at most");
DEFINE_int64(duplicator_max_failed_backoff_interval_ms, 2000,
             "Number of interval milliseconds to retry when writebatch to thirdparty "
             "storage failed");

using Tablet = kudu::tablet::Tablet;
using DuplicationMode = kudu::tablet::Tablet::DuplicationMode;

namespace kudu {
namespace duplicator {

Duplicator::Duplicator(ThreadPool* duplicate_pool, tablet::TabletReplica* tablet_replica)
    : duplicate_pool_(duplicate_pool),
      tablet_replica_(tablet_replica),
      last_confirmed_opid_(consensus::MinimumOpId()),
      stopped_(true),
      queue_(FLAGS_duplicator_max_queue_size),
      realtime_tmp_queue_(FLAGS_duplicator_max_queue_size),
      log_replayer_(new duplicator::LogReplayer(tablet_replica_)),
      need_replay_again_(false) {
  connector_manager_ = tablet_replica->connector_manager();
  duplication_mode_.store(DuplicationMode::INIT);
}

Duplicator::~Duplicator() {}

Status Duplicator::Init(const ConnectorOptions& options) {
  options_ = options;
  connector_ = connector_manager_->GetOrNewConnector(options);
  if (!connector_) {
    string message = Substitute("get connector fail, options $0", options.ToString());
    LOG_WITH_PREFIX(ERROR) << message;
    return Status::Incomplete(message);
  }
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
  // TODO(duyuqi)
  // We should check this replay task's state before switch to realtime mode.
  log_replayer_->TriggerReplayTask();
  stopped_ = false;
  return Status::OK();
}

Status Duplicator::Shutdown() {
  stopped_ = true;
  log_replayer_->Shutdown();
  if (duplicate_pool_token_) {
    duplicate_pool_token_->Shutdown();
    duplicate_pool_token_.reset();
  }
  connector_ = nullptr;
  return Status::OK();
}

Status Duplicator::Duplicate(tablet::WriteOpState* write_op_state,
                             tablet::Tablet::DuplicationMode expect_mode) {
  if (stopped_) {
    return Status::IllegalState("duplicator is stopped");
  }
  std::unique_ptr<DuplicateMsg> msg;
  // Use 'write_op_state = nullptr && expect_mode = WAL_DUPLICATION_FINISH' to
  // represent current wal replayer task has finished.
  if (write_op_state) {
    msg = std::make_unique<DuplicateMsg>(write_op_state,
                                         tablet_replica_->tablet_metadata()->table_name());
    RETURN_NOT_OK_LOG(msg->ParseKafkaRecord(), ERROR, "parse kafka record failed");
  }

  // TODO(duyuqi)
  // adding metrics for the 'duplication_mode_'.
  switch (duplication_mode_.load()) {
    case DuplicationMode::INIT: {
      LOG_WITH_PREFIX(FATAL) << "Unbelievable, a message is duplicating in INIT mode.";
      break;
    }
    case DuplicationMode::WAL_DUPLICATION: {
      consensus::OpId opid;
      if (expect_mode != DuplicationMode::WAL_DUPLICATION_FINISH) {
        opid = msg->op_id();
      }
      Status s = WorkAtWalReplay(std::move(msg), expect_mode);
      if (!s.ok()) {
        KLOG_EVERY_N_SECS(WARNING, 60) << Substitute(
            "$0, status: $1, current_mode: $2, expect_mode: $3, WorkAtWalReplay error, opid: $4",
            LogPrefix(),
            s.ToString(),
            Tablet::DuplicationMode_Name(duplication_mode_.load()),
            Tablet::DuplicationMode_Name(expect_mode),
            opid.ShortDebugString());
        return s;
      }
      break;
    }
    case DuplicationMode::WAL_DUPLICATION_FINISH: {
      // TODO(duyuqi)
      // Remove the state: DuplicationMode::WAL_DUPLICATION_FINISH ?
      Status s = WorkAtWalReplayFinished(std::move(msg), expect_mode);
      if (!s.ok()) {
        LOG_WITH_PREFIX(WARNING) << Substitute(
            "status: $0, current_mode: $1, expect_mode: $2, WorkAtWalReplayFinished error",
            s.ToString(),
            Tablet::DuplicationMode_Name(duplication_mode_.load()),
            Tablet::DuplicationMode_Name(expect_mode));
        return s;
      }
      break;
    }
    case DuplicationMode::REALTIME_DUPLICATION: {
      consensus::OpId opid = msg->op_id();
      Status s = WorkAtRealtime(std::move(msg), expect_mode);
      if (!s.ok()) {
        LOG_WITH_PREFIX(WARNING) << Substitute(
            "status: $0, current_mode: $1, expect_mode: $2, REALTIME_DUPLICATION error, opid: $3",
            s.ToString(),
            Tablet::DuplicationMode_Name(duplication_mode_.load()),
            Tablet::DuplicationMode_Name(expect_mode),
            opid.ShortDebugString());
        return s;
      }
      break;
    }
    default: {
      LOG_WITH_PREFIX(FATAL) << "unknown duplication mode";
    }
  }
  if (stopped_) {
    return Status::IllegalState("duplicator is stopped");
  }
  RETURN_NOT_OK_LOG(duplicate_pool_token_->Submit([this]() { this->Apply(); }),
                    WARNING,
                    Substitute("$0 duplicator submit apply task failed", LogPrefix()));
  return Status::OK();
}

void Duplicator::Apply() {
  vector<unique_ptr<DuplicateMsg>> duplication_msgs;
  vector<unique_ptr<DuplicateMsg>> append_msgs;
  bool should_merge_tmp_realtime = false;
  if (duplication_mode_.load() == DuplicationMode::REALTIME_DUPLICATION) {
    // If current mode is realtime and realtime_tmp_queue_ is not empty.
    // the realtime_tmp_queue should write to destination system, then
    // current queue_ write to destination.
    std::unique_lock l(realtime_tmp_lock_);
    if (!realtime_tmp_queue_.empty()) {
      int coff = 1;
      do {
        Status s = realtime_tmp_queue_.BlockingDrainTo(
            &append_msgs,
            MonoTime::Now() + MonoDelta::FromMilliseconds(coff * FLAGS_queue_drain_timeout_ms));
        if (PREDICT_TRUE(s.ok())) {
          realtime_tmp_queue_top_op_id_.Clear();
          break;
        }
        if (PREDICT_FALSE(s.IsAborted())) {
          LOG_WITH_PREFIX(WARNING) << "tserver shutdown, status: " << s.ToString();
          return;
        }
        CHECK(s.IsTimedOut());
        CHECK(append_msgs.empty());
        LOG_WITH_PREFIX(WARNING) << s.ToString() << ", timeout is unexpected, retry it";
        coff = std::min(coff << 1, 16);
      } while (true);

      should_merge_tmp_realtime = true;
    }
  }

  if (should_merge_tmp_realtime) {
    if (!queue_.empty()) {
      int coff = 1;
      do {
        Status s = queue_.BlockingDrainTo(
            &duplication_msgs,
            MonoTime::Now() + MonoDelta::FromMilliseconds(coff * FLAGS_queue_drain_timeout_ms));
        if (PREDICT_TRUE(s.ok())) {
          break;
        }
        if (PREDICT_FALSE(s.IsAborted())) {
          // program is shutdown.
          return;
        }
        CHECK(s.IsTimedOut());
        CHECK(append_msgs.empty());
        LOG_WITH_PREFIX(WARNING) << s.ToString() << ", timeout is unexpected, retry it";
        coff = std::min(coff << 1, 16);
      } while (true);
    }
    // TODO(duyuqi) remove duplicated records.
    // and check the two queue.
    duplication_msgs.reserve(duplication_msgs.size() + append_msgs.size());
    std::move(append_msgs.begin(), append_msgs.end(), std::back_inserter(duplication_msgs));
  } else {
    CHECK(append_msgs.empty());
    Status s = queue_.BlockingDrainTo(
        &duplication_msgs,
        MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_queue_drain_timeout_ms));
    if (PREDICT_FALSE(s.IsAborted())) {
      // program is shutdown.
      return;
    }
    if (PREDICT_FALSE(s.IsTimedOut())) {
      CHECK(duplication_msgs.empty());
      KLOG_EVERY_N_SECS(INFO, 60) << LogPrefix() << s.ToString()
                                  << ", nothing need to duplicate, yield duplication task";
      return;
    }
    CHECK_OK(s);
  }

  Status s;
  int64_t backoff_ms = 50;
  while (true) {
    if (stopped_) {
      return;
    }
    // TODO(duyuqi)
    // 1. Try to avoid duplicated messages because of partial failed.
    // 2. limit write batch size? produce some data and flush?
    s = connector_->WriteBatch(options_.name, duplication_msgs);
    if (s.ok()) {
      break;
    }
    KLOG_EVERY_N_SECS(WARNING, 10)
        << LogPrefix()
        << Substitute(
               "WriteBatch destination storage system error, retry it, msg size: $0 status $1",
               duplication_msgs.size(),
               s.ToString());
    SleepFor(MonoDelta::FromMilliseconds(backoff_ms));
    backoff_ms = std::min(backoff_ms << 1, FLAGS_duplicator_max_failed_backoff_interval_ms);
  }
  auto rit = duplication_msgs.rbegin();
  this->set_last_confirmed_opid((*rit)->op_id());
}

Status Duplicator::WorkAtWalReplay(unique_ptr<DuplicateMsg> msg,
                                   tablet::Tablet::DuplicationMode expect_mode) {
  CHECK(expect_mode == DuplicationMode::WAL_DUPLICATION ||
        expect_mode == DuplicationMode::WAL_DUPLICATION_FINISH ||
        expect_mode == DuplicationMode::REALTIME_DUPLICATION);
  DuplicationMode wal_mode = DuplicationMode::WAL_DUPLICATION;
  if (expect_mode == DuplicationMode::WAL_DUPLICATION_FINISH) {
    CHECK(!msg);
    if (PREDICT_TRUE(
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
  }

  if (expect_mode == DuplicationMode::WAL_DUPLICATION) {
    consensus::OpId opid = msg->op_id();
    // Avoid using BlockingPut, because it will cause apply pool stuck indrection.
    switch (queue_.Put(std::move(msg))) {
      case QueueStatus::QUEUE_SUCCESS: {
        set_queue_latest_opid(opid);
        return Status::OK();
      }
      case QueueStatus::QUEUE_FULL: {
        return Status::Incomplete("queue full, put fail when replay wals, retry put again");
      }
      case QueueStatus::QUEUE_SHUTDOWN: {
        return Status::IllegalState("queue shutdown when replay wals");
      }
      default:
        LOG_WITH_PREFIX(FATAL) << "impossble state, opid: " << opid.ShortDebugString();
        __builtin_unreachable();
    }
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
    LOG_WITH_PREFIX(INFO) << Substitute(
        "At mode $0, realtime_tmp_queue has too many elements, clear all elements, "
        "realtime_tmp_queue_ size: $1",
        Tablet::DuplicationMode_Name(DuplicationMode::WAL_DUPLICATION),
        realtime_tmp_queue_.size());
    realtime_tmp_queue_.Clear();
    realtime_tmp_queue_top_op_id_.Clear();
    need_replay_again_ = true;
  }

  return Status::OK();
}

void Duplicator::ReplayWals() {
  consensus::OpId restart_opid = queue_latest_opid();
  CHECK(restart_opid.IsInitialized());
  log_replayer_->SetStartPoint(restart_opid);
  log_replayer_->TriggerReplayTask();
}

Status Duplicator::WorkAtWalReplayFinished(unique_ptr<DuplicateMsg> msg,
                                           tablet::Tablet::DuplicationMode expect_mode) {
  CHECK(expect_mode == DuplicationMode::WAL_DUPLICATION_FINISH ||
        expect_mode == DuplicationMode::REALTIME_DUPLICATION);

  DuplicationMode wal_end_mode = DuplicationMode::WAL_DUPLICATION_FINISH;

  consensus::OpId top_op_id;
  std::unique_lock l(realtime_tmp_lock_);
  if (expect_mode == DuplicationMode::WAL_DUPLICATION_FINISH) {
    CHECK(!msg);
    if (!realtime_tmp_queue_.empty()) {
      top_op_id = realtime_tmp_queue_top_op_id_;
    }
  } else {
    if (realtime_tmp_queue_.empty()) {
      realtime_tmp_queue_top_op_id_ = msg->op_id();
      top_op_id = msg->op_id();
    } else {
      top_op_id = realtime_tmp_queue_top_op_id_;
    }
    CHECK_NE(QueueStatus::QUEUE_FULL, realtime_tmp_queue_.Put(std::move(msg)));
  }
  bool switch_success = false;
  consensus::OpId queue_latest_opid = this->queue_latest_opid();
  if ((!top_op_id.IsInitialized() && !need_replay_again_) ||
      (top_op_id.IsInitialized() && top_op_id.index() <= (queue_latest_opid.index() + 1))) {
    if (duplication_mode_.compare_exchange_strong(wal_end_mode,
                                                  DuplicationMode::REALTIME_DUPLICATION,
                                                  std::memory_order_seq_cst,
                                                  std::memory_order_seq_cst)) {
      switch_success = true;
    }
    LOG_WITH_PREFIX(INFO) << Substitute(
        "top_op_id $0, queue_latest_opid $1, switch mode from $2 to $3, need_replay_again_: $4, "
        "switch_success: $5",
        top_op_id.ShortDebugString(),
        queue_latest_opid.ShortDebugString(),
        Tablet::DuplicationMode_Name(wal_end_mode),
        Tablet::DuplicationMode_Name(DuplicationMode::REALTIME_DUPLICATION),
        need_replay_again_,
        switch_success);
    return Status::OK();
  }

  need_replay_again_ = false;
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
    switch_success = true;
    ReplayWals();
  }
  LOG_WITH_PREFIX(INFO) << strings::Substitute(
      "top_op_id $0, switch mode from $1 to $2, switch_success: $3",
      top_op_id.ShortDebugString(),
      Tablet::DuplicationMode_Name(wal_end_mode),
      Tablet::DuplicationMode_Name(DuplicationMode::WAL_DUPLICATION),
      switch_success);
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
  consensus::OpId opid = msg->op_id();
  {
    std::unique_lock<Mutex> l(realtime_tmp_lock_);
    if (!realtime_tmp_queue_.empty()) {
      CHECK(realtime_tmp_queue_top_op_id_.IsInitialized());
      switch (realtime_tmp_queue_.Put(std::move(msg))) {
        case QueueStatus::QUEUE_SUCCESS: {
          return Status::OK();
        }
        case QueueStatus::QUEUE_FULL: {
          LOG_WITH_PREFIX(INFO) << Substitute(
              "duplicator realtime_tmp_queue is full because of not processing the queue in time, "
              "will downgrade from realtime to wal_mode, current opid: $0",
              opid.ShortDebugString());
          realtime_tmp_queue_.Clear();
          realtime_tmp_queue_top_op_id_.Clear();
          CHECK(duplication_mode_.compare_exchange_strong(realtime_mode,
                                                          DuplicationMode::WAL_DUPLICATION,
                                                          std::memory_order_seq_cst,
                                                          std::memory_order_seq_cst));
          ReplayWals();
          return Status::OK();
        }
        case QueueStatus::QUEUE_SHUTDOWN: {
          return Status::IllegalState(
              Substitute("tmp queue shutdown, opid: $0", opid.ShortDebugString()));
        }
        default:
          LOG_WITH_PREFIX(FATAL) << "tmp queue unknown status, " << opid.ShortDebugString();
          __builtin_unreachable();
      }
      return Status::OK();
    }
  }
  switch (queue_.Put(std::move(msg))) {
    case QueueStatus::QUEUE_SUCCESS:
      set_queue_latest_opid(opid);
      break;
    case QueueStatus::QUEUE_FULL: {
      LOG_WITH_PREFIX(INFO) << Substitute(
          "duplicator queue full, will downgrade from realtime to wal_mode, current opid: $0",
          opid.ShortDebugString());
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
      return Status::IllegalState(Substitute("queue shutdown, opid: $0", opid.ShortDebugString()));
    }
    default:
      LOG_WITH_PREFIX(FATAL) << "Unknown status, opid: " << opid.ShortDebugString();
      __builtin_unreachable();
  }
  return Status::OK();
}

string Duplicator::LogPrefix() const {
  return Substitute(
      "duplicator info: confirmed opid $0, queue latest opid $1, tablet id $2, peer uuid $3, "
      "duplication_mode: $4 ",
      last_confirmed_opid().ShortDebugString(),
      queue_latest_opid().ShortDebugString(),
      tablet_replica_->tablet_id(),
      tablet_replica_->permanent_uuid(),
      Tablet::DuplicationMode_Name(duplication_mode_.load()));
}

}  // namespace duplicator
}  // namespace kudu
