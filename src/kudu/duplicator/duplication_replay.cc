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

#include "kudu/duplicator/duplication_replay.h"
#include <glog/logging.h>

#include <atomic>
#include <string>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/duplicator/wal_file_reader.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/pb_util.h"

using kudu::pb_util::SecureDebugString;
using std::string;
using strings::Substitute;

namespace kudu {
namespace duplicator {

WalReplay::WalReplay(tablet::TabletReplica* tablet_replica)
    : tablet_replica_(tablet_replica), replay_stopped_(true) {
  tablet::Tablet* tablet = tablet_replica_->tablet();
  const string& tablet_id = tablet->tablet_id();
  FsManager* fs_manager = tablet_replica_->tablet_metadata()->fs_manager();
  const string wal_dir = fs_manager->GetTabletWalDir(tablet_id);

  scoped_refptr<log::LogIndex> log_index(nullptr);
  log::LogReader::Open(fs_manager->env(),
                       wal_dir,
                       log_index,
                       tablet_id,
                       tablet->GetMetricEntity().get(),
                       nullptr,
                       &log_reader_);
  CHECK(log_reader_ != nullptr);
}

WalReplay::~WalReplay() {}

Status WalReplay::Init() {
  ThreadPool* replay_pool = tablet_replica_->replay_pool();
  CHECK(replay_pool != nullptr);
  thread_pool_token_ = replay_pool->NewToken(ThreadPool::ExecutionMode::SERIAL);
  return Status::OK();
}

Status WalReplay::Shutdown() {
  if (thread_pool_token_) {
    thread_pool_token_->Shutdown();
  }
  return Status::OK();
}

Status WalReplay::TriggerReplayTask(const consensus::OpId* restart_point) {
  VLOG(0) << "TriggerReplayTask";
  return thread_pool_token_->Submit([this, restart_point]() { this->Replay(restart_point); });
}

Status WalReplay::FindStartPoint(consensus::OpId* start_point) {
  log::LogAnchor anchor;
  tablet_replica_->log_anchor_registry()->Register(-1, "duplicator", &anchor);
  SCOPED_CLEANUP({ tablet_replica_->log_anchor_registry()->Unregister(&anchor); });

  log::SegmentSequence segments;
  log_reader_->GetSegmentsSnapshot(&segments);

  std::reverse(segments.begin(), segments.end());
  log::SegmentSequence replay_segments;
  bool has_start_point = false;

  // Seek the segment which Start Point in it.
  for (const scoped_refptr<log::ReadableLogSegment>& segment : segments) {
    WalFileReader reader(segment);
    log::LogEntryPB* value = nullptr;
    while ((value = reader.Next()) != nullptr) {
      if (value->has_replicate() &&
          value->replicate().op_type() == consensus::OperationType::DUPLICATE_OP) {
        const consensus::DuplicateRequestPB& duplicate = value->replicate().duplicate_request();
        *start_point = duplicate.op_id();
        if (!has_start_point) {
          has_start_point = true;
        }
      }
    }
    if (has_start_point) {
      return Status::OK();
    }
  }
  if (!has_start_point) {
    return Status::NotFound(
        Substitute("not found start point, should replay all, tablet id: $0, peer uuid: $1",
                   tablet_replica_->tablet_id(),
                   tablet_replica_->permanent_uuid()));
  }
  return Status::OK();
}

Status WalReplay::Replay(const consensus::OpId* restart_point) {
  replay_stopped_.store(false, std::memory_order_relaxed);
  // To Pretect wals, avoid wal file gced when relaying.
  VLOG(0) << Substitute("Replay, Replay start, tablet id: $0, peer uuid: $1",
                        tablet_replica_->tablet_id(),
                        tablet_replica_->permanent_uuid());
  log::LogAnchor anchor;
  tablet_replica_->log_anchor_registry()->Register(-1, "duplicator", &anchor);
  SCOPED_CLEANUP({ tablet_replica_->log_anchor_registry()->Unregister(&anchor); });

  bool start_replay = false;
  if (restart_point == nullptr) {
    start_replay = true;
  }
  log::SegmentSequence segments;
  log_reader_->GetSegmentsSnapshot(&segments);

  for (const scoped_refptr<log::ReadableLogSegment>& segment : segments) {
    WalFileReader reader(segment);
    if (!start_replay) {
      Status status = reader.IsExists(restart_point->index());
      if (status.IsNotFound()) {
        continue;
      }
    }

    log::LogEntryPB* value = nullptr;
    int64_t count = 0;
    int64_t dup_count = 0;
    while ((value = reader.Next()) != nullptr) {
      if (value->has_replicate()) {
        count++;
        consensus::ReplicateMsg& replicate_msg =
            const_cast<consensus::ReplicateMsg&>(value->replicate());
        if (!start_replay) {
          CHECK_LE(replicate_msg.id().index(), restart_point->index());
          if (replicate_msg.id().index() == restart_point->index()) {
            start_replay = true;
          }
          continue;
        }
        switch (value->replicate().op_type()) {
          case consensus::OperationType::WRITE_OP: {
            tablet::Tablet* tablet = tablet_replica_->tablet();
            tserver::WriteRequestPB* write = replicate_msg.mutable_write_request();
            std::shared_ptr<tablet::WriteOpState> op_state_ptr =
                std::make_shared<tablet::WriteOpState>(tablet_replica_, write, nullptr);
            tablet::WriteOpState& op_state = *op_state_ptr;
            op_state.mutable_op_id()->CopyFrom(replicate_msg.id());
            op_state.set_timestamp(Timestamp(replicate_msg.timestamp()));

            Schema inserts_schema;
            RETURN_NOT_OK_PREPEND(SchemaFromPB(op_state.request()->schema(), &inserts_schema),
                                  "Couldn't decode client schema");

            RETURN_NOT_OK_PREPEND(
                tablet->DecodeWriteOperations(&inserts_schema, op_state_ptr.get()),
                Substitute("Could not decode row operations: $0",
                           SecureDebugString(op_state.request()->row_operations())));
            // TODO(duyuqi), to mark a DUPLICATE_OP.
            WARN_NOT_OK(tablet->DuplicateRowOperations(
                            op_state_ptr, tablet::Tablet::DuplicationMode::WAL_DUPLICATION),
                        "DuplicateRowOperations");
            dup_count++;
            break;
          }
          case consensus::OperationType::DUPLICATE_OP:
          default:
            tablet_replica_->duplicator()->set_last_confirmed_opid(replicate_msg.id());
            break;
        }
      }
    }
    VLOG(0) << Substitute("segment replicate count: $0, replay write count: $1", count, dup_count);
  }
  VLOG(0) << Substitute("Replay finish, tablet id: $0, peer uuid: $1",
                        tablet_replica_->tablet_id(),
                        tablet_replica_->permanent_uuid());
  replay_stopped_.store(true, std::memory_order_relaxed);
  return Status::OK();
}

Status WalReplay::StopReplay() {
  replay_stopped_.store(true, std::memory_order_relaxed);
  return Status::OK();
}

}  // namespace duplicator
}  // namespace kudu
