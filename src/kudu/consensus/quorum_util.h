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
#include <unordered_set>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

enum RaftConfigState {
  PENDING_CONFIG,
  COMMITTED_CONFIG,
  ACTIVE_CONFIG,
};

bool IsRaftConfigMember(const std::string& uuid, const RaftConfigPB& config);
bool IsRaftConfigVoter(const std::string& uuid, const RaftConfigPB& config);
bool IsRaftConfigDuplicator(const std::string& uuid, const RaftConfigPB& config);

// Whether the specified Raft role is attributed to a peer which can participate
// in leader elections.
bool IsVoterRole(RaftPeerPB::Role role);

// Get the specified member of the config.
// Returns Status::NotFound if a member with the specified uuid could not be
// found in the config.
Status GetRaftConfigMember(RaftConfigPB* config,
                           const std::string& uuid,
                           RaftPeerPB** peer_pb);

// Get the leader of the consensus configuration.
// Returns Status::NotFound() if the leader RaftPeerPB could not be found in
// the config, or if there is no leader defined.
Status GetRaftConfigLeader(ConsensusStatePB* cstate, RaftPeerPB** peer_pb);

// Modifies 'configuration' remove the peer with the specified 'uuid'.
// Returns false if the server with 'uuid' is not found in the configuration.
// Returns true on success.
bool RemoveFromRaftConfig(RaftConfigPB* config, const std::string& uuid);

// Returns true iff the two peers have equivalent replica types and associated
// options.
bool ReplicaTypesEqual(const RaftPeerPB& peer1, const RaftPeerPB& peer2);

// Counts the number of voters in the configuration.
int CountVoters(const RaftConfigPB& config);
int CountMembers(const RaftConfigPB& config, RaftPeerPB::MemberType type);

// Calculates size of a configuration majority based on # of voters.
int MajoritySize(int num_voters);

// Determines the role that the peer with uuid 'peer_uuid' plays in the
// cluster. If 'peer_uuid' is empty or is not a member of the configuration,
// this function will return NON_PARTICIPANT, regardless of whether it is
// specified as the leader in 'leader_uuid'. Likewise, if 'peer_uuid' is a
// NON_VOTER in the config, this function will return LEARNER, regardless of
// whether it is specified as the leader in 'leader_uuid' (although that
// situation is illegal in practice).
RaftPeerPB::Role GetConsensusRole(const std::string& peer_uuid,
                                  const std::string& leader_uuid,
                                  const RaftConfigPB& config);

// Same as above, but uses the leader and active role from the given
// ConsensusStatePB.
RaftPeerPB::Role GetConsensusRole(const std::string& peer_uuid,
                                  const ConsensusStatePB& cstate);

// Same as above, but requires that the given 'peer' is a participant
// in the committed configuration in specified consensus state.
// If not, it will return incorrect results.
RaftPeerPB::Role GetParticipantRole(const RaftPeerPB& peer,
                                    const ConsensusStatePB& cstate);

// Verifies that the provided configuration is well formed.
Status VerifyRaftConfig(const RaftConfigPB& config);

// Superset of checks performed by VerifyRaftConfig. Also ensures that the
// leader is a configuration voter, if it is set, and that a valid term is set.
Status VerifyConsensusState(const ConsensusStatePB& cstate);

// Provide a textual description of the difference between two consensus states,
// suitable for logging.
std::string DiffConsensusStates(const ConsensusStatePB& old_state,
                                const ConsensusStatePB& new_state);

// Same as the above, but just the RaftConfigPB portion of the configuration.
std::string DiffRaftConfigs(const RaftConfigPB& old_config,
                            const RaftConfigPB& new_config);

// Return 'true' iff there is a quorum and the specified tablet configuration
// is under-replicated given the 'replication_factor', ignoring failures of
// the UUIDs in 'uuids_ignored_for_underreplication'.
//
// The decision is based on the health information provided by the Raft
// configuration in the 'config' parameter.
bool ShouldAddReplica(const RaftConfigPB& config,
                      int replication_factor,
                      const std::unordered_set<std::string>& uuids_ignored_for_underreplication =
                          std::unordered_set<std::string>());

// Return 'true' if number of duplicator less than duplication_factor.
// We support only 1 duplicator, so duplication_factor = 1.
bool ShouldAddDuplicator(const RaftConfigPB& config,
                         int duplication_factor);

// Check if the given Raft configuration contains at least one extra replica
// which should (and can) be removed in accordance with the specified
// replication factor and current Raft leader. If so, and if a healthy majority
// exists, then return 'true' and set the UUID of the best candidate for
// eviction into the 'uuid_to_evict' out parameter. Otherwise, return 'false'.
bool ShouldEvictReplica(const RaftConfigPB& config,
                        const std::string& leader_uuid,
                        int replication_factor,
                        std::string* uuid_to_evict = nullptr);

// Check if the peer is a duplicator.
bool IsDuplicator(const RaftPeerPB& peer);

// A uri like this: host3:port3,host2:port2,host1:port1,
// it is the same as host1:port1,host2:port2,host3:port3
//
// Uri standard format can simplify some logic and avoid some problems.
// For example using kafka, standard format of uri like this:
//   host1:port1,host2:port2,host3:port3
// it is the same as:
//   1. host1:port1,host2:port2,host3:port3,host2:port2 (duplicated 'host2:port2')
//   2. host3:port3,host2:port2,host1:port1 (out of order with standard format)
// and a simply format: host1,host2,host3, it should format to:
//   host1:9092,host2:9092,host3:9092, we use the default port to fill it.
//
// Normalize by sorting ip:port. If port is absent, we add a
// default port to it.
Status NormalizeUri(DownstreamType type, const std::string& uri, std::string* normalized_uri);

}  // namespace consensus
}  // namespace kudu

