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
// The file define a Duplicator, it contains common logic codes,
// User shoud pick Connector what you want.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/duplicator/connector.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {
class ThreadPool;
class ThreadPoolToken;

namespace tablet {
class TabletReplica;
class WriteOpState;
}  // namespace tablet

namespace duplicator {
class ConnectorManager;
class LogReplayer;

// Duplicator is a wrapper for Connector,
// It write ops to thirdparty destination storage system.
class Duplicator {
 public:
  Duplicator(ThreadPool* duplicate_pool,
             tablet::TabletReplica* tablet_replica);
  ~Duplicator();

  Status Init(const ConnectorOptions& options);

  Status Shutdown();

  bool is_started() const { return !stopped_; }

  Status Duplicate(tablet::WriteOpState* write_op_state,
                   tablet::Tablet::DuplicationMode expect_mode);

  consensus::OpId last_confirmed_opid() const {
    std::lock_guard<Mutex> l(mutex_);
    return last_confirmed_opid_;
  }

  void set_last_confirmed_opid(const consensus::OpId& op_id) {
    std::lock_guard<Mutex> l(mutex_);
    if (!last_confirmed_opid_.IsInitialized() ||
        consensus::OpIdLessThan(last_confirmed_opid_, op_id)) {
      last_confirmed_opid_ = op_id;
    }
  }

  LogReplayer* log_replayer() const { return log_replayer_.get(); }

  string LogPrefix() const;

 private:
  Status WorkAtWalReplay(std::unique_ptr<DuplicateMsg> msg,
                         tablet::Tablet::DuplicationMode expect_mode);
  Status WorkAtWalReplayFinished(std::unique_ptr<DuplicateMsg> msg,
                                 tablet::Tablet::DuplicationMode expect_mode);
  Status WorkAtRealtime(std::unique_ptr<DuplicateMsg> msg,
                        tablet::Tablet::DuplicationMode expect_mode);

  void ReplayWals();

  // Take messages from realtime_tmp_queue_ and queue_ and write them into
  // destination storage system.
  void Apply();

  consensus::OpId queue_latest_opid() const {
    std::lock_guard<Mutex> l(queue_latest_opid_lock_);
    return queue_latest_opid_;
  }

  void set_queue_latest_opid(const consensus::OpId& opid) {
    std::lock_guard<Mutex> l(queue_latest_opid_lock_);
    if (!queue_latest_opid_.IsInitialized() || consensus::OpIdLessThan(queue_latest_opid_, opid)) {
      queue_latest_opid_ = opid;
    }
  }

  ThreadPool* duplicate_pool_;

  tablet::TabletReplica* tablet_replica_;

  // protect 'last_confirmed_opid_'.
  mutable Mutex mutex_;

  // progress point to the destination storage system.
  consensus::OpId last_confirmed_opid_;

  std::atomic<bool> stopped_;

  mutable Mutex queue_latest_opid_lock_;
  consensus::OpId queue_latest_opid_;

  // TODO(duyuqi)
  // Add metrics for length of 'queue_' and 'realtime_tmp_queue_',
  // 'duplication_mode_', some opid infomation.
  BlockingQueue<std::unique_ptr<DuplicateMsg>> queue_;

  // protect 'realtime_tmp_queue_'.
  Mutex realtime_tmp_lock_;

  // Top element's OpId of 'realtime_tmp_queue_'.
  consensus::OpId realtime_tmp_queue_top_op_id_;

  // the realtime write should hold in 'realtime_tmp_queue_' when WAL_MODE and WAL_END_MODE.
  BlockingQueue<std::unique_ptr<DuplicateMsg>> realtime_tmp_queue_;

  std::atomic<tablet::Tablet::DuplicationMode> duplication_mode_;

  std::unique_ptr<duplicator::LogReplayer> log_replayer_;

  // connector_manager_ manages Connectors, now it includes only one connector, which is
  // KafkaConnector.
  ConnectorManager* connector_manager_;

  // A Connector is responsible to write ops to thirdparty destination storage system.
  // Default it's a kafka connector, now only support KafkaConnnector.
  Connector* connector_;
  ConnectorOptions options_;
  std::unique_ptr<ThreadPoolToken> duplicate_pool_token_;

  // To help to judge whether this duplicator should replay wals again when
  // switch to WAL_DUPLICATION_FINISH_MODE. if need_replay_again_ is true and
  // realtime_tmp_queue_top_op_id_ is not Initialized(means realtime_tmp_queue_ is empty)
  bool need_replay_again_;

  DISALLOW_COPY_AND_ASSIGN(Duplicator);
};

}  // namespace duplicator
}  // namespace kudu
