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
#include "kudu/duplicator/connector.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;
class ThreadPool;
class ThreadPoolToken;

namespace tablet {
class TabletReplica;
class WriteOpState;
}  // namespace tablet

namespace duplicator {
class ConnectorManager;

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
  void Apply();

  consensus::OpId last_confirmed_opid() const {
    std::lock_guard<std::mutex> l(mutex_);
    return last_confirmed_opid_;
  }

  void set_last_confirmed_opid(const consensus::OpId& op_id) {
    std::lock_guard<std::mutex> l(mutex_);
    last_confirmed_opid_ = op_id;
  }

  string ToString() const;

 private:
  ThreadPool* duplicate_pool_;
  tablet::TabletReplica* tablet_replica_;
  // protect 'last_confirmed_opid_'.
  mutable std::mutex mutex_;
  // progress point to the destination storage system.
  consensus::OpId last_confirmed_opid_;

  std::atomic<bool> stopped_;
  // TODO(duyuqi) use pointer to avoid copy
  BlockingQueue<std::unique_ptr<DuplicateMsg>> queue_;

  // connector_manager_ manages Connectors, now it includes only one connector, which is
  // KafkaConnector.
  ConnectorManager* connector_manager_;

  // A Connector is responsible to write ops to thirdparty destination storage system.
  // Default it's a kafka connector, now only support KafkaConnnector.
  Connector* connector_;
  ConnectorOptions options_;
  std::unique_ptr<ThreadPoolToken> duplicate_pool_token_;

  DISALLOW_COPY_AND_ASSIGN(Duplicator);
};

}  // namespace duplicator
}  // namespace kudu
