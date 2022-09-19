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

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

namespace log {
class LogReader;
}  // namespace log

namespace tablet {
class TabletReplica;
}  // namespace tablet

namespace duplicator {

// Sometimes duplicator would work at WAL_DUPLICATION mode.
// This class will read rows from wal segment files and do duplication.
class LogReplayer {
 public:
  explicit LogReplayer(tablet::TabletReplica* tablet_replica);

  ~LogReplayer() = default;

  void Init();

  void Shutdown();

  consensus::OpId start_point() const {
    return start_point_;
  }

  void SetStartPoint(const consensus::OpId& start_opid) {
    start_point_ = start_opid;
  }

  // Seek the last 'DUPLICATE_OP' record, use it as start_point_.
  Status SeekStartPoint();

  // Submit a replay wal task, the task will replay all WRITE_OP wal
  // records from 'start_point_'.
  void TriggerReplayTask();

 private:
  // Duplication from the 'start_point_' OpId.
  // If 'start_point_' is nullptr, we'll find the first
  // consensus::OperationType::DUPLICATE_OP replicate msg as restart point.
  Status Replay();

  std::shared_ptr<log::LogReader> GetLogReader();

  std::string LogPrefix() const;

  tablet::TabletReplica* tablet_replica_;

  std::unique_ptr<ThreadPoolToken> log_replay_token_;

  // All wal logs before 'start_point_' (including 'start_point_') has been duplicated to
  // the destination storage system.
  consensus::OpId start_point_;

  log::SegmentSequence last_segments_;

  std::atomic<bool> shutdown_ = false;

  DISALLOW_COPY_AND_ASSIGN(LogReplayer);
};

}  // namespace duplicator
}  // namespace kudu
