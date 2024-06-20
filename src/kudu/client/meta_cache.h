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
// This module is internal to the client and not a public API.
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/client/replica_controller-internal.h"
#include "kudu/common/partition.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {

class Sockaddr;

namespace tserver {
class TabletServerAdminServiceProxy;
class TabletServerServiceProxy;
} // namespace tserver

namespace master {
class GetTableLocationsResponsePB;
class GetTabletLocationsResponsePB;
class TSInfoPB;
class TabletLocationsPB;
} // namespace master

namespace client {

class ClientTest_TestMasterLookupPermits_Test;
class ClientTest_TestMetaCacheExpiry_Test;
class KuduClient;
class KuduTable;

namespace internal {

// The number of tablets to fetch from the master in a round trip when performing
// a lookup of a single partition (e.g. for a write), or re-looking-up a tablet with
// stale information.
const int kFetchTabletsPerPointLookup = 10;
// The number of tablets to fetch from the master when looking up a range of tablets.
const int kFetchTabletsPerRangeLookup = 1000;

////////////////////////////////////////////////////////////

class LookupRpc;
class MetaCache;
class RemoteTablet;

// The information cached about a given tablet server in the cluster.
//
// This class is thread-safe.
class RemoteTabletServer {
 public:
  explicit RemoteTabletServer(const master::TSInfoPB& pb);

  // Initialize the RPC proxy to this tablet server, if it is not already set up.
  // This will involve a DNS lookup if there is not already an active proxy.
  // If there is an active proxy, does nothing.
  void InitProxy(KuduClient* client, const StatusCallback& cb);

  // Update information from the given pb.
  // Requires that 'pb''s UUID matches this server.
  void Update(const master::TSInfoPB& pb);

  // Return the current proxy to this tablet server. Requires that InitProxy()
  // be called prior to this.
  std::shared_ptr<tserver::TabletServerServiceProxy> proxy() const;
  std::shared_ptr<tserver::TabletServerAdminServiceProxy> admin_proxy();

  std::string ToString() const;

  void GetHostPorts(std::vector<HostPort>* host_ports) const;

  // Returns the remote server's uuid.
  const std::string& permanent_uuid() const;

  // Return a copy of this tablet server's location, as assigned by the master.
  // If no location is assigned, the returned string will be empty.
  std::string location() const;

 private:
  // Internal callback for DNS resolution.
  void DnsResolutionFinished(const HostPort& hp,
                             std::vector<Sockaddr>* addrs,
                             KuduClient* client,
                             const StatusCallback& user_callback,
                             const Status &result_status);

  mutable simple_spinlock lock_;
  const std::string uuid_;
  // If not assigned, location_ will be an empty string.
  std::string location_;

  std::vector<HostPort> rpc_hostports_;

  // The path on which this server is listening for unix domain socket connections.
  // This should only be used in the case that it can be determined that the tablet
  // server is local to the client.
  std::optional<std::string> unix_domain_socket_path_;

  std::shared_ptr<tserver::TabletServerServiceProxy> proxy_;
  std::shared_ptr<tserver::TabletServerAdminServiceProxy> admin_proxy_;

  DISALLOW_COPY_AND_ASSIGN(RemoteTabletServer);
};

struct RemoteReplica {
  RemoteTabletServer* ts;
  consensus::RaftPeerPB::Role role;
  bool failed;
};

typedef std::unordered_map<std::string, std::unique_ptr<RemoteTabletServer>>
    TabletServerRegistry;
typedef std::unordered_map<std::string, RemoteTabletServer*> TabletServerMap;

// A ServerPicker for tablets servers, backed by the MetaCache.
// Replicas are returned fully initialized and ready to be used.
class MetaCacheServerPicker : public rpc::ServerPicker<RemoteTabletServer> {
 public:
  MetaCacheServerPicker(KuduClient* client,
                        scoped_refptr<MetaCache> meta_cache,
                        const KuduTable* table,
                        RemoteTablet* tablet);

  virtual ~MetaCacheServerPicker() {}
  void PickLeader(const ServerPickedCallback& callback, const MonoTime& deadline) override;

  // In the case of this MetaCacheServerPicker class, the implementation of this
  // method is very selective. It marks only servers hosting the remote tablet
  // the MetaCacheServerPicker object is bound to, not the entire RemoteTabletServer.
  void MarkServerFailed(RemoteTabletServer* replica, const Status& status) override;

  void MarkReplicaNotLeader(RemoteTabletServer* replica) override;
  void MarkResourceNotFound(RemoteTabletServer* replica) override;
 private:

  // Called whenever a tablet lookup in the metacache completes.
  void LookUpTabletCb(const ServerPickedCallback& callback,
                      const MonoTime& deadline,
                      const Status& status);

  // Called when the proxy is initialized.
  static void InitProxyCb(const ServerPickedCallback& callback,
                          RemoteTabletServer* replica,
                          const Status& status);

  // Lock protecting accesses/updates to 'followers_'.
  mutable simple_spinlock lock_;

  // Reference to the client so that we can initialize a replica proxy, when we find it.
  KuduClient* client_;

  // A ref to the meta cache.
  scoped_refptr<MetaCache> meta_cache_;

  // The table we're writing to. If null, relies on tablet ID-based lookups
  // instead of partition key-based lookups.
  const KuduTable* table_;

  // The tablet we're picking replicas for.
  RemoteTablet* const tablet_;

  // TSs that refused writes and that were marked as followers as a consequence.
  std::set<RemoteTabletServer*> followers_;
};

// The client's view of a given tablet. This object manages lookups of
// the tablet's locations, status, etc.
//
// This class is thread-safe.
class RemoteTablet : public RefCountedThreadSafe<RemoteTablet> {
 public:
  RemoteTablet(std::string tablet_id,
               Partition partition)
      : tablet_id_(std::move(tablet_id)),
        partition_(std::move(partition)),
        stale_(false) {
  }

  // Updates this tablet's replica locations.
  Status Refresh(
      const TabletServerMap& tservers,
      const master::TabletLocationsPB& locs_pb,
      const google::protobuf::RepeatedPtrField<master::TSInfoPB>& ts_info_dict);

  // Mark this tablet as stale, indicating that the cached tablet metadata is
  // out of date. Staleness is checked by the MetaCache when
  // LookupTabletByKey() is called to determine whether the fast (non-network)
  // path can be used or whether the metadata must be refreshed from the Master.
  void MarkStale();

  // Whether the tablet has been marked as stale.
  bool stale() const;

  // Mark any replicas of this tablet hosted by 'ts' as failed. They will
  // not be returned in future cache lookups.
  //
  // The provided status is used for logging.
  void MarkReplicaFailed(RemoteTabletServer *ts, const Status& status);

  // Return the number of failed replicas for this tablet.
  int GetNumFailedReplicas() const;

  // Return the tablet server which is acting as the current LEADER for
  // this tablet, provided it hasn't failed.
  //
  // Returns NULL if there is currently no leader, or if the leader has
  // failed. Given that the replica list may change at any time,
  // callers should always check the result against NULL.
  RemoteTabletServer* LeaderTServer() const;

  // Writes this tablet's TSes (across all replicas) to 'servers'. Skips
  // failed replicas.
  void GetRemoteTabletServers(std::vector<RemoteTabletServer*>* servers) const;

  // Writes this tablet's replicas to 'replicas'. Skips failed replicas.
  void GetRemoteReplicas(std::vector<RemoteReplica>* replicas) const;

  // Return true if the tablet currently has a known LEADER replica
  // (i.e the next call to LeaderTServer() is likely to return non-NULL)
  bool HasLeader() const;

  const std::string& tablet_id() const { return tablet_id_; }

  const Partition& partition() const {
    return partition_;
  }

  // Mark the specified tablet server as the leader of the consensus configuration in the cache.
  void MarkTServerAsLeader(const RemoteTabletServer* server);

  // Mark the specified tablet server as a follower in the cache.
  void MarkTServerAsFollower(const RemoteTabletServer* server);

  // Return stringified representation of the list of replicas for this tablet.
  std::string ReplicasAsString() const;

 private:
  // Same as ReplicasAsString(), except that the caller must hold lock_.
  std::string ReplicasAsStringUnlocked() const;

  const std::string tablet_id_;
  const Partition partition_;

  std::atomic<bool> stale_;

  mutable simple_spinlock lock_; // Protects replicas_.
  std::vector<RemoteReplica> replicas_;

  DISALLOW_COPY_AND_ASSIGN(RemoteTablet);
};

// MetaCacheEntry holds either a tablet and its associated `RemoteTablet`
// instance, or a non-covered partition range.
class MetaCacheEntry {
 public:
  MetaCacheEntry() { }

  // Construct a MetaCacheEntry representing a tablet.
  MetaCacheEntry(MonoTime expiration_time, scoped_refptr<RemoteTablet> tablet)
      : expiration_time_(expiration_time),
        tablet_(std::move(tablet)) {
  }

  // Construct a MetaCacheEntry representing a non-covered range with the
  // provided range partition bounds.
  MetaCacheEntry(MonoTime expiration_time,
                 PartitionKey lower_bound_partition_key,
                 PartitionKey upper_bound_partition_key)
      : expiration_time_(expiration_time),
        lower_bound_partition_key_(std::move(lower_bound_partition_key)),
        upper_bound_partition_key_(std::move(upper_bound_partition_key)) {
  }

  // Returns `true` if this is a non-covered partition range.
  bool is_non_covered_range() const {
    DCHECK(Initialized());
    return tablet_.get() == nullptr;
  }

  // Returns the remote tablet, should only be called if this entry contains a
  // tablet.
  const scoped_refptr<RemoteTablet>& tablet() const {
    DCHECK(tablet_);
    DCHECK(Initialized());
    return tablet_;
  }

  // Returns the inclusive lower bound partition key for the entry.
  const PartitionKey& lower_bound_partition_key() const {
    DCHECK(Initialized());
    if (is_non_covered_range()) {
      return lower_bound_partition_key_;
    }
    return tablet_->partition().begin();
  }

  // Returns the exclusive upper bound partition key for the entry.
  const PartitionKey& upper_bound_partition_key() const {
    DCHECK(Initialized());
    if (is_non_covered_range()) {
      return upper_bound_partition_key_;
    }
    return tablet_->partition().end();
  }

  const MonoTime& expiration_time() const {
    return expiration_time_;
  }

  void refresh_expiration_time(MonoTime expiration_time) {
    DCHECK(Initialized());
    DCHECK(expiration_time.Initialized());
    // Do not check that the new expiration time comes after the existing expiration
    // time, because that may not hold if the master changes it's configured ttl.
    expiration_time_ = expiration_time;
  }

  // Returns true if the partition key is contained in this meta cache entry.
  bool Contains(const PartitionKey& partition_key) const;

  // Returns true if this meta cache entry is stale.
  bool stale() const;

  // Returns a formatted string representation of the metacache suitable for
  // debug printing.
  //
  // This string will not be redacted, since table partitions are considered
  // metadata.
  std::string DebugString(const KuduTable* table) const;

  // Returns true if the entry is initialized.
  bool Initialized() const {
    return expiration_time_.Initialized();
  }

 private:
  // The expiration time of this cached entry.
  MonoTime expiration_time_;

  // The tablet. If this is a non-covered range then the tablet will be a nullptr.
  scoped_refptr<RemoteTablet> tablet_;

  // The lower bound partition key, if this is a non-covered range.
  PartitionKey lower_bound_partition_key_;

  // The upper bound partition key, if this is a non-covered range.
  PartitionKey upper_bound_partition_key_;
};

// Manager of RemoteTablets and RemoteTabletServers. The client consults
// this class to look up a given tablet or server.
//
// This class will also be responsible for cache eviction policies, etc.
class MetaCache : public RefCountedThreadSafe<MetaCache> {
 public:
  // The passed 'client' object must remain valid as long as MetaCache is alive.
  MetaCache(KuduClient* client, ReplicaController::Visibility replica_visibility);
  ~MetaCache() = default;

  // Determines what type of operation a MetaCache lookup is being done for.
  enum class LookupType {
    // The lookup should only return a tablet which actually covers the
    // requested partition key.
    kPoint,
    // The lookup should return the next tablet after the requested
    // partition key if the requested key does not fall within a covered
    // range.
    kLowerBound
  };

  // Look up which tablet hosts the given partition key for a table. When it is
  // available, the tablet is stored in 'remote_tablet' (if not NULL) and the
  // callback is fired. Only tablets with non-failed LEADERs are considered.
  //
  // NOTE: the callback may be called from an IO thread or inline with this
  // call if the cached data is already available.
  //
  // NOTE: the memory referenced by 'table' must remain valid until 'callback'
  // is invoked.
  void LookupTabletByKey(const KuduTable* table,
                         PartitionKey partition_key,
                         const MonoTime& deadline,
                         LookupType lookup_type,
                         scoped_refptr<RemoteTablet>* remote_tablet,
                         const StatusCallback& callback);

  // Look up the locations of the given tablet, storing the result in
  // 'remote_tablet' if not null, and calling 'lookup_complete_cb' once the
  // lookup is complete. Only tablets with non-failed LEADERs are considered.
  //
  // NOTE: the callback may be called from an IO thread or inline with this
  // call if the cached data is already available.
  void LookupTabletById(KuduClient* client,
                        const std::string& tablet_id,
                        const MonoTime& deadline,
                        scoped_refptr<RemoteTablet>* remote_tablet,
                        const StatusCallback& lookup_complete_cb);

  // Lookup the given tablet by key, only consulting local information.
  // Returns true and sets *entry if successful.
  bool LookupEntryByKeyFastPath(const KuduTable* table,
                                const PartitionKey& partition_key,
                                MetaCacheEntry* entry);
  // Lookup the given tablet by tablet ID, only consulting local information.
  // Returns true and sets *entry if successful.
  bool LookupEntryByIdFastPath(const std::string& tablet_id,
                               MetaCacheEntry* entry);

  // Process the response for the given key-based lookup parameters, indexing
  // the location information as appropriate.
  Status ProcessGetTableLocationsResponse(const KuduTable* table,
                                          const PartitionKey& partition_key,
                                          bool is_exact_lookup,
                                          const master::GetTableLocationsResponsePB& resp,
                                          MetaCacheEntry* cache_entry,
                                          int max_returned_locations);

  // Clears the non-covered range entries from a table's meta cache.
  void ClearNonCoveredRangeEntries(const std::string& table_id);

  // Clear meta-cache for the specified table identifier.
  void ClearCache();

  // Mark any replicas of any tablets hosted by 'ts' as failed. They will
  // not be returned in future cache lookups.
  void MarkTSFailed(RemoteTabletServer* ts, const Status& status);

  // Acquire or release a permit to perform a (slow) master lookup.
  //
  // If acquisition fails, caller may still do the lookup, but is first
  // blocked for a short time to prevent lookup storms.
  bool AcquireMasterLookupPermit();
  void ReleaseMasterLookupPermit();

  // Return stringified representation of the given partition key, using "<start>" if empty.
  static std::string DebugLowerBoundPartitionKey(const KuduTable* table,
                                                 const PartitionKey& partition_key);

 private:
  friend class LookupRpc;
  friend class LookupRpcById;

  FRIEND_TEST(client::ClientTest, TestMasterLookupPermits);
  FRIEND_TEST(client::ClientTest, TestMetaCacheExpiry);
  FRIEND_TEST(MetaCacheLookupStressTest, PerfSynthetic);

  // Called on the slow LookupTablet path when the master responds. Populates
  // the tablet caches and returns a reference to the first one.
  Status ProcessLookupResponse(const LookupRpc& rpc,
                               MetaCacheEntry* cache_entry,
                               int max_returned_locations);

  // Process the response for the given id-based lookup parameters, indexing
  // the location information as appropriate.
  Status ProcessGetTabletLocationsResponse(const std::string& tablet_id,
                                           const master::GetTabletLocationsResponsePB& resp,
                                           MetaCacheEntry* cache_entry);

  // Perform the complete fast-path lookup. Returns:
  // - NotFound if the lookup hits a non-covering range.
  // - Incomplete if the fast path was not possible
  // - OK if the lookup was successful.
  //
  // If 'lookup_type' is kLowerBound, then 'partition_key' will be updated to indicate the
  // start of the range for the matched tablet.
  Status DoFastPathLookup(const KuduTable* table,
                          PartitionKey* partition_key, // in-out parameter
                          LookupType lookup_type,
                          scoped_refptr<RemoteTablet>* remote_tablet);

  // Perform the fast-path lookup by tablet ID. Returns:
  // - Incomplete if there was no cache entry
  // - OK if the lookup was successful
  //
  // If 'remote_tablet' isn't null, it is populated with a pointer to the
  // RemoteTablet being looked up. Otherwise, just does the lookup, priming the
  // cache with the location.
  Status DoFastPathLookupById(const std::string& tablet_id,
                              scoped_refptr<RemoteTablet>* remote_tablet);

  // Update our information about the given tablet server.
  //
  // This is called when we get some response from the master which contains
  // the latest host/port info for a server.
  //
  // NOTE: Must be called with lock_ held.
  void UpdateTabletServerUnlocked(const master::TSInfoPB& pb);

  KuduClient* client_;

  percpu_rwlock lock_;

  // Registry of all tablet servers as a map of tablet server's
  // UUID -> std::unique_ptr<RemoteTabletServer>.
  //
  // Given that the set of tablet servers in a cluster is bounded by physical
  // machines and every tablet server has its unique identifier, we never remove
  // entries from this map until the MetaCache is destructed. Note that the
  // ClearCache() method doesn't touch this registry, but updates ts_cache_ map
  // below which contains raw pointers to the elements in this registry.
  // So, there is no need to use shared_ptr and alike for the entries.
  //
  // Protected by lock_.
  TabletServerRegistry ts_registry_;

  // Cache of Tablet Server locations: TS UUID -> RemoteTabletServer*.
  // The cache can be cleared by the ClearCache() method.
  //
  // Protected by lock_.
  TabletServerMap ts_cache_;

  // Cache entries for tablets and non-covered ranges, keyed by table ID, used
  // for key-based lookups.
  //
  // Protected by lock_.
  typedef std::map<PartitionKey, MetaCacheEntry> TabletMap;
  std::unordered_map<std::string, TabletMap> tablets_by_table_and_key_;

  // Cache entries for tablets, keyed by tablet ID, used for ID-based lookups.
  // NOTE: existence in 'tablets_by_table_and_key' does not imply existence in
  // 'entry_by_tablet_id_', and vice versa.
  //
  // Protected by lock_.
  //
  // TODO(awong): it might be nice for ID-based lookups and table-based lookups
  // to use the same entries. It's currently tricky to do so since ID-based
  // lookups don't incur any table metadata, making lookups by table ID tricky.
  std::unordered_map<std::string, MetaCacheEntry> entry_by_tablet_id_;

  // The underlying remote tablets pointed to by the above cache entry
  // containers, keyed by tablet ID. If an entry does not exist for a given
  // tablet ID in this container, none can exist in either of the above
  // containers.
  //
  // Protected by lock_
  std::unordered_map<std::string, scoped_refptr<RemoteTablet>> tablets_by_id_;

  // Prevents master lookup "storms" by delaying master lookups when all
  // permits have been acquired.
  Semaphore master_lookup_sem_;

  // Policy on tablet replica visibility: what type of replicas to expose.
  const ReplicaController::Visibility replica_visibility_;

  DISALLOW_COPY_AND_ASSIGN(MetaCache);
};

} // namespace internal
} // namespace client
} // namespace kudu
