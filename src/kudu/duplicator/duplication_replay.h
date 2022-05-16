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

#pragma once

#include <boost/container/detail/pair.hpp>
#include <memory>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet.h"

namespace kudu {

namespace tablet {
class TabletReplica;
}  // namespace tablet

namespace duplicator {

class WalReplay {
 public:
  explicit WalReplay(tablet::TabletReplica* tablet_replica);

  ~WalReplay();

  Status Init();

  Status Shutdown();

  enum class ActionType { SEEK_START_ENTRY = 0, REPLAY_ENTRY };

  struct SeekStartEntryOption {
    const consensus::OpId* start_point;
    bool* has_start_point;
  };

  struct ReplayEntryOption {};

  struct ReadSegmentOption {
    ActionType type;
    union {
      SeekStartEntryOption* start_entry;
      ReplayEntryOption* replay;
    } option;
  };

  Status FindStartPoint(consensus::OpId* start_point);

  // Duplication from the restart_point OpId.
  // If restart_point == nullptr, we'll find the first
  // consensus::OperationType::DUPLICATE_OP replicate msg as restart point.
  Status Replay(const consensus::OpId* restart_point);

  Status TriggerReplayTask(const consensus::OpId* restart_point);

  Status StopReplay();

 private:
  // unique_ptr<LogEntryPB> entry;
  std::shared_ptr<log::LogReader> log_reader_;
  tablet::TabletReplica* tablet_replica_;

  std::unique_ptr<ThreadPoolToken> thread_pool_token_;
  std::atomic_bool replay_stopped_;
};

}  // namespace duplicator
}  // namespace kudu
