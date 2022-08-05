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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/port.h>

#include "kudu/clock/clock.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnar_serialization.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/key_range.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rpc_verification_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/server_base.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/ops/alter_schema_op.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/participant_op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_coordinator.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/transactions/txn_status_manager.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_replica_lookup.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"
#include "kudu/util/trace_metrics.h"

DECLARE_double(tserver_inject_invalid_authz_token_ratio);
DECLARE_int32(scanner_default_batch_size_bytes);
DECLARE_int32(scanner_max_batch_size_bytes);

using google::protobuf::RepeatedPtrField;
using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigRequestPB;
using kudu::consensus::ChangeConfigResponsePB;
using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::consensus::GetLastOpIdRequestPB;
using kudu::consensus::GetNodeInstanceRequestPB;
using kudu::consensus::GetNodeInstanceResponsePB;
using kudu::consensus::LeaderStepDownMode;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::OpId;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::RunLeaderElectionRequestPB;
using kudu::consensus::RunLeaderElectionResponsePB;
using kudu::consensus::StartTabletCopyRequestPB;
using kudu::consensus::StartTabletCopyResponsePB;
using kudu::consensus::TimeManager;
using kudu::consensus::UnsafeChangeConfigRequestPB;
using kudu::consensus::UnsafeChangeConfigResponsePB;
using kudu::consensus::VoteRequestPB;
using kudu::consensus::VoteResponsePB;
using kudu::fault_injection::MaybeTrue;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::ParseTokenVerificationResult;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcSidecar;
using kudu::security::TokenPB;
using kudu::security::TokenVerifier;
using kudu::server::ServerBase;
using kudu::tablet::AlterSchemaOpState;
using kudu::tablet::MvccSnapshot;
using kudu::tablet::OpCompletionCallback;
using kudu::tablet::ParticipantOpState;
using kudu::tablet::Tablet;
using kudu::tablet::TABLET_DATA_COPYING;
using kudu::tablet::TABLET_DATA_DELETED;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::TabletReplica;
using kudu::tablet::TabletStatePB;
using kudu::tablet::TxnMetadataPB;

using kudu::tablet::WriteAuthorizationContext;
using kudu::tablet::WriteOpState;
using kudu::tablet::WritePrivileges;
using kudu::tablet::WritePrivilegeType;
using std::make_optional;
using std::nullopt;
using std::optional;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace cfile {
extern const char* CFILE_CACHE_MISS_BYTES_METRIC_NAME;
extern const char* CFILE_CACHE_HIT_BYTES_METRIC_NAME;
}  // namespace cfile

namespace tserver {

static const char* SCANNER_BYTES_READ_METRIC_NAME = "scanner_bytes_read";

namespace {

// Lookup the given tablet, only ensuring that it exists.
// If it does not, responds to the RPC associated with 'context' after setting
// resp->mutable_error() to indicate the failure reason.
//
// Returns true if successful.
template <class RespClass>
bool LookupTabletReplicaOrRespond(TabletReplicaLookupIf* tablet_manager,
                                  const string& tablet_id,
                                  RespClass* resp,
                                  RpcContext* context,
                                  scoped_refptr<TabletReplica>* replica) {
  Status s = tablet_manager->GetTabletReplica(tablet_id, replica);
  if (PREDICT_FALSE(!s.ok())) {
    if (s.IsServiceUnavailable()) {
      // If the tablet manager isn't initialized, the remote should check again
      // soon.
      SetupErrorAndRespond(resp->mutable_error(), s, TabletServerErrorPB::UNKNOWN_ERROR, context);
    } else {
      SetupErrorAndRespond(
          resp->mutable_error(), s, TabletServerErrorPB::TABLET_NOT_FOUND, context);
    }
    return false;
  }
  return true;
}

template <class RespClass>
void RespondTabletNotRunning(const scoped_refptr<TabletReplica>& replica,
                             TabletStatePB tablet_state,
                             RespClass* resp,
                             RpcContext* context) {
  Status s = Status::IllegalState("Tablet not RUNNING", tablet::TabletStatePB_Name(tablet_state));
  auto error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
  if (replica->tablet_metadata()->tablet_data_state() == TABLET_DATA_TOMBSTONED ||
      replica->tablet_metadata()->tablet_data_state() == TABLET_DATA_DELETED) {
    // Treat tombstoned tablets as if they don't exist for most purposes.
    // This takes precedence over failed, since we don't reset the failed
    // status of a TabletReplica when deleting it. Only tablet copy does that.
    error_code = TabletServerErrorPB::TABLET_NOT_FOUND;
  } else if (tablet_state == tablet::FAILED) {
    s = s.CloneAndAppend(replica->error().ToString());
    error_code = TabletServerErrorPB::TABLET_FAILED;
  }
  SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
}

// Check if the replica is running.
template <class RespClass>
bool CheckTabletReplicaRunningOrRespond(const scoped_refptr<TabletReplica>& replica,
                                        RespClass* resp,
                                        RpcContext* context) {
  // Check RUNNING state.
  TabletStatePB state = replica->state();
  if (PREDICT_FALSE(state != tablet::RUNNING)) {
    RespondTabletNotRunning(replica, state, resp, context);
    return false;
  }
  return true;
}

// Lookup the given tablet, ensuring that it both exists and is RUNNING.
// If it is not, responds to the RPC associated with 'context' after setting
// resp->mutable_error() to indicate the failure reason.
//
// Returns true if successful.
template <class RespClass>
bool LookupRunningTabletReplicaOrRespond(TabletReplicaLookupIf* tablet_manager,
                                         const string& tablet_id,
                                         RespClass* resp,
                                         RpcContext* context,
                                         scoped_refptr<TabletReplica>* replica) {
  if (!LookupTabletReplicaOrRespond(tablet_manager, tablet_id, resp, context, replica)) {
    return false;
  }
  if (!CheckTabletReplicaRunningOrRespond(*replica, resp, context)) {
    return false;
  }
  return true;
}

template <class ReqClass, class RespClass>
bool CheckUuidMatchOrRespond(TabletReplicaLookupIf* tablet_manager,
                             const char* method_name,
                             const ReqClass* req,
                             RespClass* resp,
                             RpcContext* context) {
  const string& local_uuid = tablet_manager->NodeInstance().permanent_uuid();
  if (PREDICT_FALSE(!req->has_dest_uuid())) {
    // Maintain compat in release mode, but complain.
    string msg = Substitute("$0: Missing destination UUID in request from $1: $2",
                            method_name,
                            context->requestor_string(),
                            SecureShortDebugString(*req));
#ifdef NDEBUG
    KLOG_EVERY_N(ERROR, 100) << msg;
#else
    LOG(DFATAL) << msg;
#endif
    return true;
  }
  if (PREDICT_FALSE(req->dest_uuid() != local_uuid)) {
    Status s =
        Status::InvalidArgument(Substitute("$0: Wrong destination UUID requested. "
                                           "Local UUID: $1. Requested UUID: $2",
                                           method_name,
                                           local_uuid,
                                           req->dest_uuid()));
    LOG(WARNING) << s.ToString() << ": from " << context->requestor_string() << ": "
                 << SecureShortDebugString(*req);
    SetupErrorAndRespond(resp->mutable_error(), s, TabletServerErrorPB::WRONG_SERVER_UUID, context);
    return false;
  }
  return true;
}

template <class RespClass>
bool GetConsensusOrRespond(const scoped_refptr<TabletReplica>& replica,
                           RespClass* resp,
                           RpcContext* context,
                           shared_ptr<RaftConsensus>* consensus_out) {
  shared_ptr<RaftConsensus> tmp_consensus = replica->shared_consensus();
  if (!tmp_consensus) {
    Status s =
        Status::ServiceUnavailable("Raft Consensus unavailable", "Tablet replica not initialized");
    SetupErrorAndRespond(
        resp->mutable_error(), s, TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return false;
  }
  *consensus_out = std::move(tmp_consensus);
  return true;
}

template <class RespClass>
bool CheckTabletServerNotQuiescingOrRespond(const TabletServer* server,
                                            RespClass* resp,
                                            RpcContext* context) {
  if (PREDICT_FALSE(server->quiescing())) {
    Status s = Status::ServiceUnavailable("Tablet server is quiescing");
    SetupErrorAndRespond(
        resp->mutable_error(), s, TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return false;
  }
  return true;
}

Status GetTabletRef(const scoped_refptr<TabletReplica>& replica,
                    shared_ptr<Tablet>* tablet,
                    TabletServerErrorPB::Code* error_code) {
  *DCHECK_NOTNULL(tablet) = replica->shared_tablet();
  if (PREDICT_FALSE(!*tablet)) {
    *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
    return Status::IllegalState("Tablet is not running");
  }
  return Status::OK();
}

template <class RespType>
void HandleUnknownError(const Status& s, RespType* resp, RpcContext* context) {
  resp->Clear();
  SetupErrorAndRespond(resp->mutable_error(), s, TabletServerErrorPB::UNKNOWN_ERROR, context);
}

template <class ReqType, class RespType>
void HandleResponse(const ReqType* req, RespType* resp, RpcContext* context, const Status& s) {
  if (PREDICT_FALSE(!s.ok())) {
    HandleUnknownError(s, resp, context);
    return;
  }
  context->RespondSuccess();
}

}  // namespace

// Populates 'required_column_privileges' with the column-level privileges
// required to perform the scan specified by 'scan_pb', consulting the column
// IDs found in 'schema'.
//
// Users of NewScanRequestPB (e.g. Scans and Checksums) require the following
// privileges:
//   if no projected columns (i.e. a "counting" scan) ||
//       projected columns has virtual column (e.g. "diff" scan):
//     SCAN ON TABLE || foreach (column): SCAN ON COLUMN
//   else:
//     if uses pk (e.g. ORDERED scan, or primary key fields set):
//       foreach(primary key column): SCAN ON COLUMN
//     foreach(projected column): SCAN ON COLUMN
//     foreach(predicated column): SCAN ON COLUMN
//
// Returns false if the request is malformed (e.g. unknown non-virtual column
// name), and sends an error response via 'context' if so. 'req_type' is used
// to add context in logs.
static bool GetScanPrivilegesOrRespond(const NewScanRequestPB& scan_pb,
                                       const Schema& schema,
                                       const string& req_type,
                                       unordered_set<ColumnId>* required_column_privileges,
                                       RpcContext* context) {
  const auto respond_not_authorized = [&](const string& col_name) {
    LOG(WARNING) << Substitute("rejecting $0 request from $1: no column named '$2'",
                               req_type,
                               context->requestor_string(),
                               col_name);
    context->RespondRpcFailure(ErrorStatusPB::FATAL_UNAUTHORIZED,
                               Status::NotAuthorized(Substitute("not authorized to $0", req_type)));
  };
  // If there is no projection (i.e. this is a "counting" scan), the user
  // needs full scan privileges on the table.
  if (scan_pb.projected_columns_size() == 0) {
    *required_column_privileges =
        unordered_set<ColumnId>(schema.column_ids().begin(), schema.column_ids().end());
    return true;
  }
  unordered_set<ColumnId> required_privileges;
  // Determine the scan's projected key column IDs.
  for (int i = 0; i < scan_pb.projected_columns_size(); i++) {
    optional<ColumnSchema> projected_column;
    Status s = ColumnSchemaFromPB(scan_pb.projected_columns(i), &projected_column);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << s.ToString();
      context->RespondRpcFailure(ErrorStatusPB::ERROR_INVALID_REQUEST, s);
      return false;
    }
    // A projection may contain virtual columns, which don't exist in the
    // tablet schema. If we were to search for a virtual column, we would
    // incorrectly get a "not found" error. To reconcile this with the fact
    // that we want to return an authorization error if the user has requested
    // a non-virtual column that doesn't exist, we require full scan privileges
    // for virtual columns.
    if (projected_column->type_info()->is_virtual()) {
      *required_column_privileges =
          unordered_set<ColumnId>(schema.column_ids().begin(), schema.column_ids().end());
      return true;
    }
    int col_idx = schema.find_column(projected_column->name());
    if (col_idx == Schema::kColumnNotFound) {
      respond_not_authorized(scan_pb.projected_columns(i).name());
      return false;
    }
    EmplaceIfNotPresent(&required_privileges, schema.column_id(col_idx));
  }
  // Ordered scans and any scans that make use of the primary key require
  // privileges to scan across all primary key columns.
  if (scan_pb.order_mode() == ORDERED || scan_pb.has_start_primary_key() ||
      scan_pb.has_stop_primary_key() || scan_pb.has_last_primary_key()) {
    const auto& key_cols = schema.get_key_column_ids();
    required_privileges.insert(key_cols.begin(), key_cols.end());
  }
  // Determine the scan's predicate column IDs.
  for (int i = 0; i < scan_pb.column_predicates_size(); i++) {
    int col_idx = schema.find_column(scan_pb.column_predicates(i).column());
    if (col_idx == Schema::kColumnNotFound) {
      respond_not_authorized(scan_pb.column_predicates(i).column());
      return false;
    }
    EmplaceIfNotPresent(&required_privileges, schema.column_id(col_idx));
  }
  // Do the same for the DEPRECATED_range_predicates field. Even though this
  // field is deprecated, it is still exposed as a part of our public API and
  // thus needs to be taken into account.
  for (int i = 0; i < scan_pb.deprecated_range_predicates_size(); i++) {
    int col_idx = schema.find_column(scan_pb.deprecated_range_predicates(i).column().name());
    if (col_idx == Schema::kColumnNotFound) {
      respond_not_authorized(scan_pb.deprecated_range_predicates(i).column().name());
      return false;
    }
    EmplaceIfNotPresent(&required_privileges, schema.column_id(col_idx));
  }
  *required_column_privileges = std::move(required_privileges);
  return true;
}

// Checks the column-level privileges required to perform the scan specified by
// 'scan_pb' against the authorized column IDs listed in
// 'authorized_column_ids', consulting the column IDs found in 'schema'.
//
// Returns false if the scan isn't authorized and uses 'context' to send an
// error response. 'req_type' is used for logging'.
static bool CheckScanPrivilegesOrRespond(const NewScanRequestPB& scan_pb,
                                         const Schema& schema,
                                         const unordered_set<ColumnId>& authorized_column_ids,
                                         const string& req_type,
                                         RpcContext* context) {
  unordered_set<ColumnId> required_column_privileges;
  if (!GetScanPrivilegesOrRespond(
          scan_pb, schema, req_type, &required_column_privileges, context)) {
    return false;
  }
  for (const auto& required_col_id : required_column_privileges) {
    if (!ContainsKey(authorized_column_ids, required_col_id)) {
      LOG(WARNING) << Substitute(
          "rejecting $0 request from $1: authz token doesn't "
          "authorize column ID $2",
          req_type,
          context->requestor_string(),
          required_col_id);
      context->RespondRpcFailure(
          ErrorStatusPB::FATAL_UNAUTHORIZED,
          Status::NotAuthorized(Substitute("not authorized to $0", req_type)));
      return false;
    }
  }
  return true;
}

// Returns false if the table ID of 'privilege' doesn't match 'table_id',
// responding with an error via 'context' if so. Otherwise, returns true.
// 'req_type' is used for logging purposes.
static bool CheckMatchingTableIdOrRespond(const security::TablePrivilegePB& privilege,
                                          const string& table_id,
                                          const string& req_type,
                                          RpcContext* context) {
  if (privilege.table_id() != table_id) {
    LOG(WARNING) << Substitute("rejecting $0 request from $1: '$2', expected '$3'",
                               req_type,
                               context->requestor_string(),
                               privilege.table_id(),
                               table_id);
    context->RespondRpcFailure(
        ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("authorization token is for the wrong table ID"));
    return false;
  }
  return true;
}

// Returns false if the privilege has neither full scan privileges nor any
// column-level scan privileges, in which case any scan-like request should be
// rejected. Otherwise returns true, and returns any column-level scan
// privileges in 'privilege'.
static bool CheckMayHaveScanPrivilegesOrRespond(const security::TablePrivilegePB& privilege,
                                                const string& req_type,
                                                unordered_set<ColumnId>* authorized_column_ids,
                                                RpcContext* context) {
  DCHECK(authorized_column_ids);
  DCHECK(authorized_column_ids->empty());
  if (privilege.column_privileges_size() > 0) {
    for (const auto& col_id_and_privilege : privilege.column_privileges()) {
      if (col_id_and_privilege.second.scan_privilege()) {
        EmplaceOrDie(authorized_column_ids, col_id_and_privilege.first);
      }
    }
  }
  if (privilege.scan_privilege() || !authorized_column_ids->empty()) {
    return true;
  }
  LOG(WARNING) << Substitute(
      "rejecting $0 request from $1: no column privileges", req_type, context->requestor_string());
  context->RespondRpcFailure(ErrorStatusPB::FATAL_UNAUTHORIZED,
                             Status::NotAuthorized(Substitute("not authorized to $0", req_type)));
  return false;
}

// Verifies the authorization token's correctness. Returns false and sends an
// appropriate response if the request's authz token is invalid.
template <class AuthorizableRequest>
static bool VerifyAuthzTokenOrRespond(const TokenVerifier& token_verifier,
                                      const AuthorizableRequest& req,
                                      RpcContext* context,
                                      TokenPB* token) {
  DCHECK(token);
  if (!req.has_authz_token()) {
    context->RespondRpcFailure(ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
                               Status::NotAuthorized("no authorization token presented"));
    return false;
  }
  TokenPB token_pb;
  const auto result = token_verifier.VerifyTokenSignature(req.authz_token(), &token_pb);
  ErrorStatusPB::RpcErrorCodePB error;
  Status s = ParseTokenVerificationResult(
      result, ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN, &error);
  if (!s.ok()) {
    context->RespondRpcFailure(error, s.CloneAndPrepend("authz token verification failure"));
    return false;
  }
  if (!token_pb.has_authz() || !token_pb.authz().has_table_privilege() ||
      token_pb.authz().username() != context->remote_user().username()) {
    context->RespondRpcFailure(ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
                               Status::NotAuthorized("invalid authorization token presented"));
    return false;
  }
  if (MaybeTrue(FLAGS_tserver_inject_invalid_authz_token_ratio)) {
    context->RespondRpcFailure(ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
                               Status::NotAuthorized("INJECTED FAILURE"));
    return false;
  }
  *token = std::move(token_pb);
  return true;
}

static void SetupErrorAndRespond(TabletServerErrorPB* error,
                                 const Status& s,
                                 TabletServerErrorPB::Code code,
                                 RpcContext* context) {
  // Non-authorized errors will drop the connection.
  if (code == TabletServerErrorPB::NOT_AUTHORIZED) {
    DCHECK(s.IsNotAuthorized());
    context->RespondRpcFailure(ErrorStatusPB::FATAL_UNAUTHORIZED, s);
    return;
  }
  // Generic "service unavailable" errors will cause the client to retry later.
  if ((code == TabletServerErrorPB::UNKNOWN_ERROR || code == TabletServerErrorPB::THROTTLED) &&
      s.IsServiceUnavailable()) {
    context->RespondRpcFailure(ErrorStatusPB::ERROR_SERVER_TOO_BUSY, s);
    return;
  }

  StatusToPB(s, error->mutable_status());
  error->set_code(code);
  context->RespondNoCache();
}

// Generic interface to handle scan results.
class ScanResultCollector {
 public:
  virtual void HandleRowBlock(Scanner* scanner, const RowBlock& row_block) = 0;

  // Returns number of bytes which will be returned in the response.
  virtual int64_t ResponseSize() const = 0;

  // Return the number of rows actually returned to the client.
  virtual int64_t NumRowsReturned() const = 0;

  // Initialize the serializer with the given row format flags.
  //
  // This is a separate function instead of a constructor argument passed to specific
  // collector implementations because, currently, the collector is built before the
  // request is decoded and checked for 'row_format_flags'.
  //
  // Does nothing by default.
  virtual Status InitSerializer(uint64_t /* row_format_flags */,
                                const Schema& /* scanner_schema */,
                                const Schema& /* client_schema */) {
    return Status::OK();
  }

  CpuTimes* cpu_times() { return &cpu_times_; }

 private:
  CpuTimes cpu_times_;
};

namespace {

// Given a RowBlock, set last_primary_key to the primary key of the last selected row
// in the RowBlock. If no row is selected, last_primary_key is not set.
void SetLastRow(const RowBlock& row_block, faststring* last_primary_key) {
  // Find the last selected row and save its encoded key.
  const SelectionVector* sel = row_block.selection_vector();
  if (sel->AnySelected()) {
    for (int i = sel->nrows() - 1; i >= 0; i--) {
      if (sel->IsRowSelected(i)) {
        RowBlockRow last_row = row_block.row(i);
        const Schema* schema = last_row.schema();
        schema->EncodeComparableKey(last_row, last_primary_key);
        break;
      }
    }
  }
}

// Interface for serializing results into a scan response.
class ResultSerializer {
 public:
  virtual ~ResultSerializer() = default;

  // Add the given RowBlock to the pending response.
  virtual int SerializeRowBlock(const RowBlock& row_block,
                                const Schema* client_projection_schema) = 0;

  // Return the approximate size (in bytes) of the pending response. Once this
  // result is greater than the client's requested batch size, the pending rows
  // will be returned to the client.
  virtual size_t ResponseSize() const = 0;

  // Serialize the pending rows into the response protobuf.
  // Must be called at most once.
  virtual void SetupResponse(RpcContext* context, ScanResponsePB* resp) = 0;
};

class RowwiseResultSerializer : public ResultSerializer {
 public:
  RowwiseResultSerializer(int batch_size_bytes, uint64_t flags)
      : rows_data_(batch_size_bytes * 11 / 10),
        indirect_data_(batch_size_bytes * 11 / 10),
        pad_unixtime_micros_to_16_bytes_(flags & RowFormatFlags::PAD_UNIX_TIME_MICROS_TO_16_BYTES) {
    // TODO(todd): use a chain of faststrings instead of a single one to avoid
    // allocating this large buffer. Large buffer allocations are slow and
    // potentially wasteful.
  }

  int SerializeRowBlock(const RowBlock& row_block,
                        const Schema* client_projection_schema) override {
    // TODO(todd) create some kind of serializer object that caches the projection
    // information to avoid recalculating it on every SerializeRowBlock call.
    int num_selected = kudu::SerializeRowBlock(row_block,
                                               client_projection_schema,
                                               &rows_data_,
                                               &indirect_data_,
                                               pad_unixtime_micros_to_16_bytes_);
    rowblock_pb_.set_num_rows(rowblock_pb_.num_rows() + num_selected);
    return num_selected;
  }

  size_t ResponseSize() const override { return rows_data_.size() + indirect_data_.size(); }

  void SetupResponse(RpcContext* context, ScanResponsePB* resp) override {
    CHECK(!done_);
    done_ = true;

    *resp->mutable_data() = std::move(rowblock_pb_);
    // Add sidecar data to context and record the returned indices.
    int rows_idx;
    CHECK_OK(context->AddOutboundSidecar(RpcSidecar::FromFaststring((std::move(rows_data_))),
                                         &rows_idx));
    resp->mutable_data()->set_rows_sidecar(rows_idx);

    // Add indirect data as a sidecar, if applicable.
    if (indirect_data_.size() > 0) {
      int indirect_idx;
      CHECK_OK(context->AddOutboundSidecar(RpcSidecar::FromFaststring(std::move(indirect_data_)),
                                           &indirect_idx));
      resp->mutable_data()->set_indirect_data_sidecar(indirect_idx);
    }
  }

 private:
  RowwiseRowBlockPB rowblock_pb_;
  faststring rows_data_;
  faststring indirect_data_;
  bool pad_unixtime_micros_to_16_bytes_;
  bool done_ = false;
};

class ColumnarResultSerializer : public ResultSerializer {
 public:
  static Status Create(uint64_t flags,
                       int batch_size_bytes,
                       const Schema& scanner_schema,
                       const Schema& client_schema,
                       unique_ptr<ResultSerializer>* serializer) {
    if (flags & ~RowFormatFlags::COLUMNAR_LAYOUT) {
      return Status::InvalidArgument("Row format flags not supported with columnar layout");
    }
    serializer->reset(
        new ColumnarResultSerializer(scanner_schema, client_schema, batch_size_bytes));
    return Status::OK();
  }

  int SerializeRowBlock(const RowBlock& row_block, const Schema* /* unused */) override {
    CHECK(!done_);
    int n_sel = results_.AddRowBlock(row_block);
    num_rows_ += n_sel;
    return n_sel;
  }

  size_t ResponseSize() const override {
    CHECK(!done_);

    int total = 0;
    for (const auto& col : results_.columns()) {
      total += col.data.size();
      if (col.varlen_data) {
        total += col.varlen_data->size();
      }
      if (col.non_null_bitmap) {
        total += col.non_null_bitmap->size();
      }
    }
    return total;
  }

  void SetupResponse(RpcContext* context, ScanResponsePB* resp) override {
    CHECK(!done_);
    done_ = true;
    ColumnarRowBlockPB* data = resp->mutable_columnar_data();
    auto cols = std::move(results_).TakeColumns();
    for (auto& col : cols) {
      auto* col_pb = data->add_columns();
      int sidecar_idx;
      CHECK_OK(context->AddOutboundSidecar(RpcSidecar::FromFaststring((std::move(col.data))),
                                           &sidecar_idx));
      col_pb->set_data_sidecar(sidecar_idx);

      if (col.varlen_data) {
        CHECK_OK(context->AddOutboundSidecar(
            RpcSidecar::FromFaststring((std::move(*col.varlen_data))), &sidecar_idx));
        col_pb->set_varlen_data_sidecar(sidecar_idx);
      }

      if (col.non_null_bitmap) {
        CHECK_OK(context->AddOutboundSidecar(
            RpcSidecar::FromFaststring((std::move(*col.non_null_bitmap))), &sidecar_idx));
        col_pb->set_non_null_bitmap_sidecar(sidecar_idx);
      }
    }
    data->set_num_rows(num_rows_);
  }

 private:
  ColumnarResultSerializer(const Schema& scanner_schema,
                           const Schema& client_schema,
                           int batch_size_bytes)
      : results_(scanner_schema, client_schema, batch_size_bytes) {}

  int64_t num_rows_ = 0;
  ColumnarSerializedBatch results_;
  bool done_ = false;
};

}  // anonymous namespace

// Copies the scan result to the given row block PB and data buffers.
//
// This implementation is used in the common case where a client is running
// a scan and the data needs to be returned to the client.
//
// (This is in contrast to some other ScanResultCollector implementation that
// might do an aggregation or gather some other types of statistics via a
// server-side scan and thus never need to return the actual data.)
class ScanResultCopier : public ScanResultCollector {
 public:
  explicit ScanResultCopier(int batch_size_bytes)
      : batch_size_bytes_(batch_size_bytes), num_rows_returned_(0) {}

  void HandleRowBlock(Scanner* scanner, const RowBlock& row_block) override {
    int num_selected =
        serializer_->SerializeRowBlock(row_block, scanner->client_projection_schema());

    if (num_selected > 0) {
      num_rows_returned_ += num_selected;
      scanner->add_num_rows_returned(num_selected);
      SetLastRow(row_block, &last_primary_key_);
    }
  }

  // Returns number of bytes buffered to return.
  int64_t ResponseSize() const override { return serializer_->ResponseSize(); }

  int64_t NumRowsReturned() const override { return num_rows_returned_; }

  Status InitSerializer(uint64_t row_format_flags,
                        const Schema& scanner_schema,
                        const Schema& client_schema) override {
    if (serializer_) {
      // TODO(todd) for the NewScanner case, this gets called twice
      // which is a bit ugly. Refactor to avoid!
      return Status::OK();
    }
    if (row_format_flags & COLUMNAR_LAYOUT) {
      return ColumnarResultSerializer::Create(
          row_format_flags, batch_size_bytes_, scanner_schema, client_schema, &serializer_);
    }
    serializer_.reset(new RowwiseResultSerializer(batch_size_bytes_, row_format_flags));
    return Status::OK();
  }

  void SetupResponse(RpcContext* context, ScanResponsePB* resp) {
    if (serializer_) {
      serializer_->SetupResponse(context, resp);
    }

    // Set the last row found by the collector.
    //
    // We could have an empty batch if all the remaining rows are filtered by the
    // predicate, in which case do not set the last row.
    if (last_primary_key_.length() > 0) {
      resp->set_last_primary_key(last_primary_key_.ToString());
    }
  }

 private:
  int batch_size_bytes_;
  int64_t num_rows_returned_;
  faststring last_primary_key_;
  unique_ptr<ResultSerializer> serializer_;

  DISALLOW_COPY_AND_ASSIGN(ScanResultCopier);
};

// Checksums the scan result.
class ScanResultChecksummer : public ScanResultCollector {
 public:
  ScanResultChecksummer()
      : crc_(crc::GetCrc32cInstance()), agg_checksum_(0), rows_checksummed_(0) {}

  virtual void HandleRowBlock(Scanner* scanner, const RowBlock& row_block) OVERRIDE {
    const Schema* client_projection_schema = scanner->client_projection_schema();
    if (!client_projection_schema) {
      client_projection_schema = row_block.schema();
    }

    size_t nrows = row_block.nrows();
    for (size_t i = 0; i < nrows; i++) {
      if (!row_block.selection_vector()->IsRowSelected(i)) continue;
      uint32_t row_crc = CalcRowCrc32(*client_projection_schema, row_block.row(i));
      agg_checksum_ += row_crc;
      rows_checksummed_++;
    }
    // Find the last selected row and save its encoded key.
    SetLastRow(row_block, &encoded_last_row_);
  }

  // Returns a constant -- we only return checksum based on a time budget.
  virtual int64_t ResponseSize() const OVERRIDE { return sizeof(agg_checksum_); }

  virtual int64_t NumRowsReturned() const OVERRIDE { return 0; }

  int64_t rows_checksummed() const { return rows_checksummed_; }

  // Accessors for initializing / setting the checksum.
  void set_agg_checksum(uint64_t value) { agg_checksum_ = value; }
  uint64_t agg_checksum() const { return agg_checksum_; }

 private:
  // Calculates a CRC32C for the given row.
  uint32_t CalcRowCrc32(const Schema& projection, const RowBlockRow& row) {
    tmp_buf_.clear();

    for (size_t j = 0; j < projection.num_columns(); j++) {
      uint32_t col_index = static_cast<uint32_t>(j);  // For the CRC.
      tmp_buf_.append(&col_index, sizeof(col_index));
      ColumnBlockCell cell = row.cell(j);
      if (cell.is_nullable()) {
        uint8_t is_defined = cell.is_null() ? 0 : 1;
        tmp_buf_.append(&is_defined, sizeof(is_defined));
        if (!is_defined) continue;
      }
      if (cell.typeinfo()->physical_type() == BINARY) {
        const Slice* data = reinterpret_cast<const Slice*>(cell.ptr());
        tmp_buf_.append(data->data(), data->size());
      } else {
        tmp_buf_.append(cell.ptr(), cell.size());
      }
    }

    uint64_t row_crc = 0;
    crc_->Compute(tmp_buf_.data(), tmp_buf_.size(), &row_crc, nullptr);
    return static_cast<uint32_t>(row_crc);  // CRC32 only uses the lower 32 bits.
  }

  faststring tmp_buf_;
  crc::Crc* const crc_;
  uint64_t agg_checksum_;
  int64_t rows_checksummed_;
  faststring encoded_last_row_;

  DISALLOW_COPY_AND_ASSIGN(ScanResultChecksummer);
};

// Return the batch size to use for a given request, after clamping
// the user-requested request within the server-side allowable range.
// This is only a hint, really more of a threshold since returned bytes
// may exceed this limit, but hopefully only by a little bit.
static size_t GetMaxBatchSizeBytesHint(const ScanRequestPB* req) {
  if (!req->has_batch_size_bytes()) {
    return FLAGS_scanner_default_batch_size_bytes;
  }

  VLOG(0) << "DUYUQI MARK: req->batch_size_bytes(): " << req->batch_size_bytes();
  return std::min(req->batch_size_bytes(),
                  implicit_cast<uint32_t>(FLAGS_scanner_max_batch_size_bytes));
}

namespace {
void SetResourceMetrics(const RpcContext* context,
                        const CpuTimes* cpu_times,
                        ResourceMetricsPB* metrics) {
  metrics->set_cfile_cache_miss_bytes(
      context->trace()->metrics()->GetMetric(cfile::CFILE_CACHE_MISS_BYTES_METRIC_NAME));
  metrics->set_cfile_cache_hit_bytes(
      context->trace()->metrics()->GetMetric(cfile::CFILE_CACHE_HIT_BYTES_METRIC_NAME));

  metrics->set_bytes_read(context->trace()->metrics()->GetMetric(SCANNER_BYTES_READ_METRIC_NAME));

  rpc::InboundCallTiming timing;
  timing.time_handled = context->GetTimeHandled();
  timing.time_received = context->GetTimeReceived();
  timing.time_completed = MonoTime::Now();

  metrics->set_queue_duration_nanos(timing.QueueDuration().ToNanoseconds());
  metrics->set_total_duration_nanos(timing.TotalDuration().ToNanoseconds());
  metrics->set_cpu_system_nanos(cpu_times->system);
  metrics->set_cpu_user_nanos(cpu_times->user);
}
}  // anonymous namespace

}  // namespace tserver
}  // namespace kudu
