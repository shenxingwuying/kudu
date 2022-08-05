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

#include "kudu/util/seda.h"

namespace kudu {
namespace seda {

const char* Action2String(Action action) {
  switch (action) {
    case Action::kTestStart:
      return "test_start";
    case Action::kTestAdd:
      return "test_add";
    case Action::kTestStop:
      return "test_stop";
    case Action::kMasterLeaderRebalance:
      return "master_leader_rebalance";
    case Action::kMasterReplicaRebalance:
      return "master_replica_rebalance";
    case Action::kTServerFollowerWaitIndex:
      return "tserver_follower_wait_index";
    case Action::kTServerNewScan:
      return "tserver_new_scan";
    case Action::kTServerContinueScan:
      return "tserver_continue_scan";
    default:
      return "unknown";
  }
}
}  // namespace seda
}  // namespace kudu
