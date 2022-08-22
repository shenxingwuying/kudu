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

#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {
class Thread;
}  // namespace kudu

namespace kudu {

namespace collector {


class NodesChecker : public RefCounted<NodesChecker> {
 public:
  NodesChecker();
  ~NodesChecker();

  Status Init();
  Status Start();
  void Shutdown();

  static std::string ToString();

  std::vector<std::string> GetMasters();
  std::vector<std::string> GetTServers();
  std::string GetFirstMaster();
  std::string GetFirstTServer();

  enum class TableReplicationFactorHealthStatus {
    // The table replication factor is healthy.
    HEALTHY,

    // The table replication factor is unhealthy.
    UNHEALTHY,
  };

 private:
  friend class RefCounted<NodesChecker>;

  FRIEND_TEST(TestNodesChecker, TestExtractServerHealthStatus);
  FRIEND_TEST(TestNodesChecker, TestExtractTableHealthStatus);

  Status StartNodesCheckerThread();
  void NodesCheckerThread();

  void UpdateAndCheckNodes();
  Status UpdateNodes();
  Status UpdateServers(const std::string& role);
  static Status CheckNodes();
  static Status ReportNodesMetrics(const std::string& data);

  static cluster_summary::ServerHealth ExtractServerHealthStatus(const std::string& health);
  static cluster_summary::HealthCheckResult ExtractTableHealthStatus(const std::string& health);

  static const char kMaster[];
  static const char kTserver[];

  bool initialized_;

  CountDownLatch stop_background_threads_latch_;
  scoped_refptr<Thread> nodes_checker_thread_;

  mutable RWMutex nodes_lock_;
  std::vector<std::string> tserver_http_addrs_;
  std::vector<std::string> master_http_addrs_;

  DISALLOW_COPY_AND_ASSIGN(NodesChecker);
};
} // namespace collector
} // namespace kudu
