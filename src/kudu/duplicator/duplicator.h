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

#include <memory>
#include <mutex>
#include <string>

#include "kudu/consensus/opid.pb.h"
#include "kudu/duplicator/connector.h"
#include "kudu/duplicator/duplication_replay.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;
class ThreadPool;
class ThreadPoolToken;

namespace tablet {
class WriteOpState;
struct RowOp;
}  // namespace tablet

namespace duplicator {

class Duplicator {
 public:
  Duplicator(ThreadPool* thread_pool, tablet::TabletReplica* tablet_replica);
  ~Duplicator();
  Duplicator(const Duplicator& /* dup */) = delete;
  void operator=(const Duplicator& /* dup */) = delete;

  Status Init();
  Status Shutdown();

  void Duplicate(const std::shared_ptr<tablet::WriteOpState>& op_state_ptr,
                 const tablet::RowOp* row_op,
                 const std::shared_ptr<Schema>& schema,
                 tablet::Tablet::DuplicationMode expect_mode =
                     tablet::Tablet::DuplicationMode::REALTIME_DUPLICATION);
  void Apply();

  consensus::OpId last_confirmed_opid() const {
    std::lock_guard<std::mutex> l(mutex_);
    return last_confirmed_opid_;
  }

  void set_last_confirmed_opid(const consensus::OpId& op_id) {
    std::lock_guard<std::mutex> l(mutex_);
    last_confirmed_opid_ = op_id;
  }

  WalReplay* wal_replay() { return wal_replay_; }

  string ToString();

 private:
  ThreadPool* duplicate_pool_;
  // std::shared_ptr<tablet::Tablet> tablet_;
  tablet::TabletReplica* tablet_replica_;
  consensus::OpId last_confirmed_opid_;
  mutable std::mutex mutex_;
  bool stopped_;
  // TODO(duyuqi) use pointer to avoid copy
  BlockingQueue<std::shared_ptr<DuplicateMsg>> queue_;

  std::atomic<tablet::Tablet::DuplicationMode> duplication_mode_;
  duplicator::WalReplay* wal_replay_;

  // Singleton, eg KafkaConnector
  Connector* connector_;
  std::unique_ptr<ThreadPoolToken> duplicate_pool_token_;
};

}  // namespace duplicator
}  // namespace kudu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
