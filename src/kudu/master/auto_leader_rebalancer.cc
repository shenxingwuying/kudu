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

#include "kudu/master/auto_leader_rebalancer.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using std::map;
using std::nullopt;
using std::string;
using std::vector;

using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::LeaderStepDownMode;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::RaftPeerPB;

using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using strings::Substitute;

DEFINE_uint32(auto_leader_rebalancing_rpc_timeout_seconds, 10,
              "auto leader rebalancing send leader step down rpc timeout seconds");

DEFINE_uint32(auto_leader_rebalancing_interval_seconds, 3600,
              "How long to sleep in between auto leader rebalancing cycles, before checking "
              "the cluster again to see if there is leader skew and if run task again.");

DECLARE_bool(auto_leader_rebalancing_enabled);

namespace kudu {
namespace master {

AutoLeaderRebalancerTask::AutoLeaderRebalancerTask(CatalogManager* catalog_manager,
                                                   TSManager* ts_manager)
    : catalog_manager_(catalog_manager),
      ts_manager_(ts_manager),
      shutdown_(1),
      random_generator_(random_device_()),
      number_of_loop_iterations_for_test_(0),
      moves_scheduled_this_round_for_test_(0) {}

AutoLeaderRebalancerTask::~AutoLeaderRebalancerTask() { Shutdown(); }

Status AutoLeaderRebalancerTask::Init() {
  DCHECK(!thread_) << "AutoleaderRebalancerTask is already initialized";
  RETURN_NOT_OK(MessengerBuilder("auto-leader-rebalancer").Build(&messenger_));
  return Thread::Create("catalog manager", "auto-leader-rebalancer",
                        [this]() { this->RunLoop(); }, &thread_);
}

void AutoLeaderRebalancerTask::Shutdown() {
  CHECK(thread_) << "AutoLeaderRebalancerTask is not initialized";
  if (!shutdown_.CountDown()) {
    return;
  }
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
  thread_.reset();
}

Status AutoLeaderRebalancerTask::RunLeaderRebalanceForTable(
    const scoped_refptr<TableInfo>& table_info,
    const vector<string>& tserver_uuids,
    AutoLeaderRebalancerTask::ExecuteMode mode) {
  TableMetadataLock table_l(table_info.get(), LockMode::READ);
  const SysTablesEntryPB& table_data = table_info->metadata().state().pb;
  if (table_data.state() == SysTablesEntryPB::REMOVED || table_data.num_replicas() <= 1) {
    // Don't worry about rebalancing replicas that belong to deleted tables.
    return Status::OK();
  }

  // tablet_id of leader -> uuid
  map<string, string> leader_uuid_map;
  // tablet_id of follower -> vector<uuid>
  map<string, vector<string>> follower_uuid_map;
  // tserver uuid -> vector<all leaders' tablet id>
  map<string, vector<string>> uuid_leaders_map;
  // tserver uuid -> vector<all replicas' tablet id>
  map<string, vector<string>> uuid_replicas_map;

  map<string, HostPort> leader_uuid_host_port_map;

  vector<scoped_refptr<TabletInfo>> tablet_infos;
  table_info->GetAllTablets(&tablet_infos);

  // step 1. Get basic statistics
  for (const auto& tablet : tablet_infos) {
    TabletMetadataLock tablet_l(tablet.get(), LockMode::READ);

    // Retrieve all replicas of the tablet.
    TabletLocationsPB locs_pb;
    CatalogManager::TSInfosDict ts_infos_dict;

    {
      CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager_);
      RETURN_NOT_OK(leaderlock.first_failed_status());
      // This will only return tablet replicas in the RUNNING state, and
      // filter to only retrieve voter replicas.
      RETURN_NOT_OK(catalog_manager_->GetTabletLocations(
          tablet->id(), ReplicaTypeFilter::VOTER_REPLICA, &locs_pb, &ts_infos_dict, nullopt));
    }

    // Build a summary for each replica of the tablet.
    for (const auto& r : locs_pb.interned_replicas()) {
      int index = r.ts_info_idx();
      const TSInfoPB& ts_info = *(ts_infos_dict.ts_info_pbs()[index]);
      string uuid = ts_info.permanent_uuid();
      if (r.role() == RaftPeerPB::LEADER) {
        auto& leader_uuids = LookupOrInsert(&uuid_leaders_map, uuid, {});
        leader_uuids.emplace_back(tablet->id());
        leader_uuid_map.insert({tablet->id(), uuid});
        leader_uuid_host_port_map.insert({uuid, HostPortFromPB(ts_info.rpc_addresses(0))});
      } else if (r.role() == RaftPeerPB::FOLLOWER) {
        auto& follower_uuids = LookupOrInsert(&follower_uuid_map, tablet->id(), {});
        follower_uuids.emplace_back(uuid);
      } else {
        VLOG(0) << Substitute("Not a VOTER, role: $0", RaftPeerPB::Role_Name(r.role()));
        continue;
      }

      auto& uuid_replicas = LookupOrInsert(&uuid_replicas_map, ts_info.permanent_uuid(), {});
      uuid_replicas.emplace_back(tablet->id());
    }
  }

  // step 2.
  // pick the servers which number of leaders greater than 1/3 of number of all replicas
  // <uuid, number of replica, number of leader>
  map<string, std::pair<int32_t, int32_t>> tserver_statistics;
  // uuid->leader should transfer count
  map<string, int32_t> leader_transfer_source;
  for (const auto& uuid : tserver_uuids) {
    const vector<string>& replicas = FindWithDefault(uuid_replicas_map, uuid,  {});
    uint32_t replica_count = replicas.size();
    if (replica_count == 0) {
      // means no replicas (and no leaders), maybe a tserver joined kudu cluster just now, skip it
      continue;
    }
    const vector<string>& leaders = FindWithDefault(uuid_leaders_map, uuid,  {});
    uint32_t leader_count = leaders.size();
    tserver_statistics.insert({uuid, std::pair<int32_t, int32_t>(replica_count, leader_count)});
    VLOG(1) << Substitute(
        "uuid: $0, replica_count: $1, leader_count: $2", uuid, replica_count, leader_count);

    // Our target is every tserver' replicas, number of leader : number of follower is
    // 1 : (replica_refactor -1). The constant 1 is a coarse-grained correction factor to help
    // leader rebalancer to converge stable.
    uint32_t should_transfer_count = leader_count - (replica_count / table_data.num_replicas() + 1);
    if (should_transfer_count > 0) {
      leader_transfer_source.insert({uuid, should_transfer_count});
      VLOG(1) << Substitute("$0 should transfer leader count: $1", uuid, should_transfer_count);
    }
  }

  // Step 3.
  // Generate transfer task, <tablet_id, from_uuid, to_uuid>
  map<string, std::pair<string, string>> leader_transfer_tasks;
  for (const auto& from_info : leader_transfer_source) {
    string leader_uuid = from_info.first;
    int32_t expected_count = from_info.second;
    int32_t pick_count = 0;
    vector<string>& uuid_leaders = uuid_leaders_map[leader_uuid];
    std::shuffle(uuid_leaders.begin(), uuid_leaders.end(), random_generator_);
    // pick count leader, and pick dest uuid
    for (int i = 0; i < uuid_leaders.size(); i++) {
      const string& tablet_id = uuid_leaders[i];
      vector<string> uuid_followers = follower_uuid_map[tablet_id];

      // TabletId leader transfer, pick a dest follower
      string dest_follower_uuid;
      if (uuid_followers.size() + 1 < table_data.num_replicas()) {
        continue;
      }
      double min_score = 1;
      for (int j = 0; j < uuid_followers.size(); j++) {
        std::pair<int32_t, int32_t>& pair = tserver_statistics[uuid_followers[j]];
        int32_t replica_count = pair.first;
        if (replica_count <= 0) {
          dest_follower_uuid.clear();
          break;
        }
        int32_t leader_count = pair.second;
        // double is not precise.
        double score = static_cast<double>(leader_count) / replica_count;
        if (score < min_score) {
          min_score = score;
          dest_follower_uuid = uuid_followers[j];
        }
      }
      if (dest_follower_uuid.empty()) {
        continue;
      }
      std::pair<int32_t, int32_t>& pair = tserver_statistics[leader_uuid];
      int32_t replica_count = pair.first;
      int32_t leader_count = pair.second;
      double leader_score = static_cast<double>(leader_count) / replica_count;
      if (min_score > leader_score) {
        // Skip it, because the transfer will cause more leader skew
        continue;
      }

      leader_transfer_tasks.insert(
          {tablet_id, std::pair<string, string>(leader_uuid, dest_follower_uuid)});
      tserver_statistics[leader_uuid].second--;
      tserver_statistics[dest_follower_uuid].second++;
      if (++pick_count == expected_count) {
        // Have picked enough leader transfer tasks
        break;
      }
    }
  }

  if (mode == AutoLeaderRebalancerTask::ExecuteMode::TEST) {
    if (!leader_transfer_tasks.empty()) {
      return Status::IllegalState(Substitute("leader_transfer_task size should be 0, but $0",
                                             leader_transfer_tasks.size()));
    }
    return Status::OK();
  }

  moves_scheduled_this_round_for_test_ = leader_transfer_tasks.size();
  VLOG(1) << Substitute("leader rebalance tasks, size: $0, leader_transfer_source, size: $1",
                        moves_scheduled_this_round_for_test_.load(),
                        leader_transfer_source.size());
  // Step 4. Do Leader transfer tasks.
  // @TODO(duyuqi). optimal speed
  // if the num of tasks is vary large, synchronized rpc will be slow,
  // causing the thread cost much time to do the job.
  for (const auto& task : leader_transfer_tasks) {
    string leader_uuid = task.second.first;
    LeaderStepDownRequestPB request;
    request.set_dest_uuid(task.second.first);
    request.set_tablet_id(task.first);
    request.set_mode(LeaderStepDownMode::GRACEFUL);
    request.set_new_leader_uuid(task.second.second);

    LeaderStepDownResponsePB response;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_auto_leader_rebalancing_rpc_timeout_seconds));

    auto it = leader_uuid_host_port_map.find(leader_uuid);
    if (it == leader_uuid_host_port_map.end()) {
      continue;
    }
    std::shared_ptr<TSDescriptor> leader_desc;
    if (!ts_manager_->LookupTSByUUID(leader_uuid, &leader_desc)) {
      continue;
    }

    vector<Sockaddr> resolved;
    RETURN_NOT_OK(it->second.ResolveAddresses(&resolved));
    ConsensusServiceProxy proxy(messenger_, resolved[0], it->second.host());
    RETURN_NOT_OK(proxy.LeaderStepDown(request, &response, &rpc));
    if (!response.has_error()) {
      VLOG(1) << strings::Substitute("leader transfer table: $0, tablet_id: $1, from: $2 to: $3",
                                     table_data.name(),
                                     task.first,
                                     leader_uuid,
                                     task.second.second);
    }
  }
  VLOG(0) << Substitute("table: $0, leader rebalance finish", table_data.name());
  return Status::OK();
}

Status AutoLeaderRebalancerTask::RunLeaderRebalancer() {
  MutexLock auto_lock(running_mutex_);

  // If catalog manager isn't initialized or isn't the leader, don't do leader
  // rebalancing. Putting the auto-rebalancer to sleep shouldn't affect the
  // master's ability to become the leader. When the thread wakes up and
  // discovers it is now the leader, then it can begin auto-rebalancing.
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
    if (!l.first_failed_status().ok()) {
      moves_scheduled_this_round_for_test_ = 0;
      return Status::OK();
    }
  }

  number_of_loop_iterations_for_test_++;

  // Leader balance need not disk capacity, so
  // we get all tserver uuids
  TSDescriptorVector descriptors;
  ts_manager_->GetAllDescriptors(&descriptors);

  vector<string> tserver_uuids;
  for (const auto& e : descriptors) {
    if (e->PresumedDead()) {
      continue;
    }
    tserver_uuids.emplace_back(e->permanent_uuid());
  }

  vector<scoped_refptr<TableInfo>> table_infos;
  {
    CatalogManager::ScopedLeaderSharedLock leader_lock(catalog_manager_);
    RETURN_NOT_OK(leader_lock.first_failed_status());
    catalog_manager_->GetAllTables(&table_infos);
  }
  for (const auto& table_info : table_infos) {
    RunLeaderRebalanceForTable(table_info, tserver_uuids);
  }
  LOG(INFO) << "all tables leader rebalance finish";
  return Status::OK();
}

void AutoLeaderRebalancerTask::RunLoop() {
  while (
      !shutdown_.WaitFor(MonoDelta::FromSeconds(FLAGS_auto_leader_rebalancing_interval_seconds))) {
    if (FLAGS_auto_leader_rebalancing_enabled) {
      WARN_NOT_OK(RunLeaderRebalancer(),
                  Substitute("Something wrong, maybe the master instance isn't leader"));
    }
  }
}

}  // namespace master
}  // namespace kudu
