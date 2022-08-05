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
