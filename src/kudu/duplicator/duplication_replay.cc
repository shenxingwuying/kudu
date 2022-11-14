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

#include "kudu/duplicator/duplication_replay.h"

#include <functional>
#include <ostream>
#include <string>
#include <vector>

#include <boost/range/adaptor/reversed.hpp>
#include <glog/logging.h>

#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/duplicator/connector.h"
#include "kudu/duplicator/duplicator.h"
#include "kudu/duplicator/log_segment_reader.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/threadpool.h"

using kudu::pb_util::SecureDebugString;
using std::shared_ptr;
using std::string;
using strings::Substitute;

namespace kudu {
namespace duplicator {

LogReplayer::LogReplayer(tablet::TabletReplica* tablet_replica)
    : tablet_replica_(tablet_replica),
      task_id_(-1), state_(State::STARTING) {
}

shared_ptr<log::LogReader> LogReplayer::GetLogReader() {
  tablet::Tablet* tablet = tablet_replica_->tablet();
  FsManager* fs_manager = tablet_replica_->tablet_metadata()->fs_manager();
  const string wal_dir = fs_manager->GetTabletWalDir(tablet->tablet_id());

  scoped_refptr<log::LogIndex> log_index(nullptr);
  shared_ptr<log::LogReader> log_reader;
  CHECK_OK(log::LogReader::Open(fs_manager->env(),
                                wal_dir,
                                log_index,
                                tablet->tablet_id(),
                                tablet->GetMetricEntity().get(),
                                nullptr,
                                &log_reader));
  CHECK(log_reader != nullptr);
  return log_reader;
}

void LogReplayer::Init() {
  ThreadPool* replay_pool = tablet_replica_->consensus()->replay_pool();
  CHECK(replay_pool != nullptr);
  log_replay_token_ = replay_pool->NewToken(ThreadPool::ExecutionMode::SERIAL);
}

void LogReplayer::Shutdown() {
  if (log_replay_token_) {
    log_replay_token_->Shutdown();
    log_replay_token_.reset();
  }
}

Status LogReplayer::TriggerReplayTask(int64_t task_id) {
  return log_replay_token_->Submit([this, task_id]() { this->Replay(task_id); });
}

Status LogReplayer::SeekStartPoint() {
  CHECK(!start_point_.IsInitialized());
  log::LogAnchor anchor;
  tablet_replica_->log_anchor_registry()->Register(0, "duplicator", &anchor);
  SCOPED_CLEANUP({ tablet_replica_->log_anchor_registry()->Unregister(&anchor); });

  shared_ptr<log::LogReader> log_reader = GetLogReader();
  log::SegmentSequence segments;
  log_reader->GetSegmentsSnapshot(&segments);

  // Generally, the lastest segment has DUPLICATE_OP, and we find
  // the lastest DUPLICATE_OP log entry.
  for (const auto& segment : boost::adaptors::reverse(segments)) {
    LogSegmentReader reader(segment);
    log::LogEntryPB* value = nullptr;
    while ((value = reader.Next()) != nullptr) {
      if (value->has_replicate() &&
          value->replicate().op_type() == consensus::OperationType::DUPLICATE_OP) {
        const consensus::DuplicateRequestPB& duplicate = value->replicate().duplicate_request();
        start_point_ = duplicate.op_id();
      }
    }
    if (start_point_.IsInitialized()) {
      return Status::OK();
    }
  }
  CHECK(!start_point_.IsInitialized());
  return Status::NotFound(
      Substitute("not found start point, should replay all, tablet id: $0, peer uuid: $1",
                 tablet_replica_->tablet_id(),
                 tablet_replica_->permanent_uuid()));
}

Status LogReplayer::Replay(int64_t task_id) {
  CHECK_LT(task_id_, task_id);
  CHECK(state_ == State::STARTING || state_ == State::FINISHED);
  task_id_ = task_id;
  state_ = State::RUNNING;

  // To Protect wals, avoid wal file gced when relaying.
  VLOG(0) << Substitute(
      "Replay start, tablet id: $0, peer uuid: $1, replay start point: $2, task_id: $3",
      tablet_replica_->tablet_id(),
      tablet_replica_->permanent_uuid(),
      start_point_.ShortDebugString(),
      task_id_);
  log::LogAnchor anchor;
  tablet_replica_->log_anchor_registry()->Register(0, "duplicator", &anchor);
  SCOPED_CLEANUP({ tablet_replica_->log_anchor_registry()->Unregister(&anchor); });

  // TODO(duyuqi): remove 'start_replay' instead of index >= start_point_?
  //
  // Whether start replaying.
  // Find 'start_point_' and replay wals from next entry.
  bool start_replay = false;
  if (start_point_.index() == consensus::MinimumOpId().index()) {
    start_replay = true;
  }
  shared_ptr<log::LogReader> log_reader = GetLogReader();
  log::SegmentSequence segments;
  log_reader->GetSegmentsSnapshot(&segments);
  for (const scoped_refptr<log::ReadableLogSegment>& segment : segments) {
    LogSegmentReader reader(segment);
    if (!start_replay && !reader.IsExists(start_point_.index())) {
      // This segment has no 'start_point_'.
      continue;
    }

    log::LogEntryPB* value = nullptr;
    int64_t replicate_count = 0;
    int64_t dup_write_count = 0;
    int64_t ops_count = 0;
    while ((value = reader.Next()) != nullptr) {
      if (!value->has_replicate()) {
        // Ignore commit record.
        continue;
      }
      replicate_count++;
      consensus::ReplicateMsg& replicate_msg =
          const_cast<consensus::ReplicateMsg&>(value->replicate());
      if (!start_replay) {
        CHECK_LE(replicate_msg.id().index(), start_point_.index());
        if (replicate_msg.id().index() == start_point_.index()) {
          start_replay = true;
        }
        // If find 'start_point_', replay and duplicate wal entries from next entry (skip this
        // entry). If not find 'start_point_', continue check next entry.
        continue;
      }
      switch (value->replicate().op_type()) {
        case consensus::OperationType::WRITE_OP: {
          tablet::Tablet* tablet = tablet_replica_->tablet();
          tserver::WriteRequestPB* write = replicate_msg.mutable_write_request();
          auto op_state_ptr =
              std::make_unique<tablet::WriteOpState>(tablet_replica_, write, nullptr);
          op_state_ptr->mutable_op_id()->CopyFrom(replicate_msg.id());
          op_state_ptr->set_timestamp(Timestamp(replicate_msg.timestamp()));

          Schema inserts_schema;
          RETURN_NOT_OK_PREPEND(SchemaFromPB(op_state_ptr->request()->schema(), &inserts_schema),
                                "Couldn't decode client schema");

          RETURN_NOT_OK_PREPEND(
              tablet->DecodeWriteOperations(&inserts_schema, op_state_ptr.get()),
              Substitute("Could not decode row operations: $0",
                         SecureDebugString(op_state_ptr->request()->row_operations())));
          while (true) {
            Status status = tablet_replica_->Duplicate(
                op_state_ptr.get(), tablet::Tablet::DuplicationMode::WAL_DUPLICATION);
            if (status.ok()) {
              break;
            }
            if (status.IsIncomplete()) {
              // TODO(duyuqi) exponiential backoff.
              SleepFor(MonoDelta::FromMilliseconds(100));
              continue;
            }
            // Other status (program is shutdown)
            return status;
          }
          dup_write_count++;
          ops_count += op_state_ptr->row_ops().size();
          break;
        }
        case consensus::OperationType::DUPLICATE_OP: {
          consensus::DuplicateRequestPB* duplicate_request =
              replicate_msg.mutable_duplicate_request();
          if (duplicate_request->has_op_id()) {
            tablet_replica_->duplicator()->set_last_confirmed_opid(duplicate_request->op_id());
            tablet_replica_->set_duplicator_last_committed_opid(duplicate_request->op_id());
          }
          break;
        }
        default: {
          // TODO(duyuqi), Not strict correct. But noop and change is specical.
          // Maybe we should refact the codes of noop and change replica.
          tablet_replica_->duplicator()->set_last_confirmed_opid(replicate_msg.id());
          tablet_replica_->set_duplicator_last_committed_opid(replicate_msg.id());
          break;
        }
      }
      start_point_.CopyFrom(replicate_msg.id());
    }
    CHECK(start_replay);
    VLOG(0) << Substitute("segment replicate count: $0, replay write count: $1, ops size: $2",
                          replicate_count,
                          dup_write_count,
                          ops_count);
  }

  VLOG(0) << Substitute("Replay finish, tablet id: $0, peer uuid: $1, start point: $2, task_id: $3",
                        tablet_replica_->tablet_id(),
                        tablet_replica_->permanent_uuid(),
                        start_point_.ShortDebugString(),
                        task_id_);
  state_ = State::FINISHED;
  last_segments_.swap(segments);
  return Status::OK();
}

}  // namespace duplicator
}  // namespace kudu
