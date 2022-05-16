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
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <cppkafka/exceptions.h>
#include <cppkafka/metadata.h>
#include <cppkafka/producer.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/duplicator/duplication_replay.h"
#include "kudu/duplicator/kafka/kafka_connector.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/monotime.h"
#include "kudu/util/threadpool.h"

namespace kudu {
class Schema;

namespace tablet {
struct RowOp;
}  // namespace tablet
}  // namespace kudu

using strings::Substitute;

DEFINE_string(downstream_type, "kafka", "");
DEFINE_int64(duplicate_pool_timeout_s, 10, "");
DEFINE_int64(duplicator_max_queue_size, 8192, "the duplicator's max queue size");

static bool ValidateDownstreamFlag(const char* /*flagname*/, const std::string& value) {
  if (value == "kafka" || value == "local_kudu") {
    return true;
  }
  LOG(ERROR) << "duplication downstream_type should be: kafka, local_kudu";
  return false;
}
DEFINE_validator(downstream_type, &ValidateDownstreamFlag);

using DuplicationMode = kudu::tablet::Tablet::DuplicationMode;

namespace kudu {
namespace duplicator {

Duplicator::Duplicator(ThreadPool* thread_pool, tablet::TabletReplica* tablet_replica)
    : duplicate_pool_(thread_pool),
      tablet_replica_(tablet_replica),
      last_confirmed_opid_(consensus::MinimumOpId()),
      stopped_(true),
      queue_(FLAGS_duplicator_max_queue_size) {
  // #ifdef ENABLE_DUPLICATION
  // TODO(duyuqi), we should enable it when needed.
  // default disable the duplication function.

  if (FLAGS_downstream_type == "kafka") {
    connector_ = kafka::KafkaConnector::GetInstance();
    wal_replay_ = new duplicator::WalReplay(tablet_replica_);
  } else {
    // Not Supported
    connector_ = nullptr;
    wal_replay_ = nullptr;
  }
  duplication_mode_.store(DuplicationMode::WAL_DUPLICATION);

  // #else
  // Not Support
  //    connector_ = nullptr;
  //#endif
}
Duplicator::~Duplicator() {
  // @TODO(duyuqi)
  // no release connector, for its a shared object
  connector_ = nullptr;
  if (wal_replay_ != nullptr) {
    delete wal_replay_;
    wal_replay_ = nullptr;
  }
}

Status Duplicator::Init() {
  stopped_ = false;
  connector_->Init();
  duplicate_pool_token_ =
      duplicate_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL
                                // TODO(duyuqi) metric_entity for duplicate_pool_
      );
  duplication_mode_.store(DuplicationMode::WAL_DUPLICATION);
  wal_replay_->Init();
  consensus::OpId last_confirmed_opid;
  Status status = wal_replay_->FindStartPoint(&last_confirmed_opid);
  if (status.ok()) {
    LOG(INFO) << "find last_confirmed_opid: " << last_confirmed_opid.DebugString();
    wal_replay_->Replay(&last_confirmed_opid);
  } else {
    LOG(WARNING) << Substitute(
        "no find confirmed duplicated point, replay from the start wal, status: $0, duplicator: $1",
        status.ToString(),
        ToString());
    wal_replay_->Replay(nullptr);
  }
  duplication_mode_.store(DuplicationMode::REALTIME_DUPLICATION);
  VLOG(0) << "duplicator init finish, in REALTIME_DUPLICATION, " << ToString()
          << ", last_confirmed_opid_: " << last_confirmed_opid_.DebugString();
  return Status::OK();
}

Status Duplicator::Shutdown() {
  stopped_ = true;
  if (duplicate_pool_token_) {
    duplicate_pool_token_->Shutdown();
  }
  wal_replay_->Shutdown();
  return Status::OK();
}

void Duplicator::Duplicate(const std::shared_ptr<tablet::WriteOpState>& write_op_state_ptr,
                           const tablet::RowOp* row_op,
                           const std::shared_ptr<Schema>& schema,
                           DuplicationMode expect_mode) {
  if (stopped_) {
    return;
  }

  std::shared_ptr<DuplicateMsg> msg = std::make_shared<DuplicateMsg>(
      write_op_state_ptr, row_op, schema, tablet_replica_->tablet_metadata()->table_name());
  consensus::OpId op_id = write_op_state_ptr->op_id();

  DuplicationMode realtime_mode = DuplicationMode::REALTIME_DUPLICATION;
  DuplicationMode wal_mode = DuplicationMode::WAL_DUPLICATION;

  if (duplication_mode_.load(std::memory_order_relaxed) != expect_mode) {
    if (expect_mode == DuplicationMode::REALTIME_DUPLICATION) {
      if (op_id.index() == (last_confirmed_opid_.index() + 1)) {
        // Should switch mode, wal_mode ->  realtime_mode.
        VLOG(0) << "Switch duplication_mode, From wal_mode to realtime_mode." << ToString();
        wal_replay_->StopReplay();
        if (duplication_mode_.compare_exchange_strong(
                wal_mode, realtime_mode, std::memory_order_relaxed, std::memory_order_relaxed)) {
          queue_.Clear();
          // std::vector<std::shared_ptr<DuplicateMsg>> msgs;
          // queue_.BlockingDrainTo(&msgs, MonoTime::Now() + MonoDelta::FromSeconds(3));
        } else {
          VLOG(0) << Substitute("ignore the message, duplicator: $0", ToString());
          return;
        }
      } else {
        VLOG(0) << Substitute(
            "Keep at wal_mode, continue replay wals, ignore the message, duplicator: $0",
            ToString());
        return;
      }
    } else {
      VLOG(0) << Substitute(
          "We realtime mode, ignore wal mode the message, it's impossible, duplicator: $0",
          ToString());
      return;
    }
  } else {
    VLOG(0) << Substitute("expected_mode: $0, matched.",
                          tablet::Tablet::DuplicationMode_Name(expect_mode));
  }

  switch (queue_.Put(msg)) {
    case QueueStatus::QUEUE_SUCCESS:
      break;
    case QueueStatus::QUEUE_FULL: {
      // If QUEUE_FULL, should switch to inc wal duplication mode.

      // Step 1. Switch mode to inc realtime duplication mode.
      // Queue is full, the msg would be dropped.
      // The duplication mode should downgrade to wal_mode.
      if (duplication_mode_.compare_exchange_strong(realtime_mode,
                                                    DuplicationMode::WAL_DUPLICATION,
                                                    std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
        VLOG(0) << "Switch duplication_mode, From realtime_mode to wal_mode." << ToString();
        {
          // Drop all queue messages.
          queue_.Clear();
          // std::vector<std::shared_ptr<DuplicateMsg>> msgs;
          // queue_.BlockingDrainTo(&msgs, MonoTime::Now() + MonoDelta::FromSeconds(3));
        }

        consensus::OpId restart_opid = last_confirmed_opid();
        // TODO(duyuqi), Replay task maybe a heavy task, split it to light tasks later.
        // Step 2, seek the start point.
        // Step 3, replay the wal entries from the start point(exclude the start point entry)
        // to lastest wal entry.
        wal_replay_->TriggerReplayTask(&restart_opid);

        // TODO(duyuqi), last_confirmed_opid_ should be duriable when wal mode realtime for
        // efficient. But now it won't work.
        return;
      }
      // Wait and try to put.
      queue_.BlockingPut(msg);
    } break;
    case QueueStatus::QUEUE_SHUTDOWN:
    default:
      return;
  }
  WARN_NOT_OK(duplicate_pool_token_->Submit([this]() { this->Apply(); }),
              "duplicator apply failed");
}

void Duplicator::Apply() {
  std::vector<std::shared_ptr<DuplicateMsg>> msgs;
  queue_.BlockingDrainTo(&msgs, MonoTime::Now() + MonoDelta::FromSeconds(30));
  if (!msgs.empty()) {
    LOG(INFO) << "msg size: " << msgs.size();
    // sync call
    Status s = connector_->WriteBatch(msgs);
    if (s.ok()) {
      auto rit = msgs.rbegin();
      {
        std::lock_guard<std::mutex> l(mutex_);
        last_confirmed_opid_ = (*rit)->op_state->op_id();
      }
      LOG(INFO) << "duplicator confirmed opid: (" << last_confirmed_opid_.term() << ","
                << last_confirmed_opid_.index() << ")";
    } else {
      LOG(WARNING) << s.ToString();
    }
  }
}

string Duplicator::ToString() {
  return Substitute("tablet id: $0, peer uuid: $1",
                    tablet_replica_->tablet_id(),
                    tablet_replica_->permanent_uuid());
}
}  // namespace duplicator
}  // namespace kudu
