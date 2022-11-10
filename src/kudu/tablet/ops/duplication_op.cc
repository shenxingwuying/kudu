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

#include "kudu/tablet/ops/duplication_op.h"

#include <memory>
#include <ostream>
#include <utility>

#include <glog/logging.h>
#include <google/protobuf/arena.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tablet {

using consensus::CommitMsg;
using consensus::DriverType;
using consensus::ReplicateMsg;
using pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using strings::Substitute;

void DuplicationOpState::SetError(const Status& s) {
  CHECK(!s.ok()) << "Expected an error status";
  error_ = OperationResultPB();
  StatusToPB(s, error_->mutable_failed_status());
}

string DuplicationOpState::ToString() const {
  return Substitute(
      "DuplicationOpState "
      "[timestamp=$0, schema=$1, request=$2, error=$3]",
      has_timestamp() ? timestamp().ToString() : "<unassigned>",
      error_ ? SecureShortDebugString(error_->failed_status()) : "(none)");
}

DuplicationOp::DuplicationOp(unique_ptr<DuplicationOpState> state,
                             consensus::DriverType type,
                             consensus::OpId last_confirmed_opid,
                             consensus::DuplicationInfoPB dup_info)
    : Op(type, Op::DUPLICATION_OP),
      state_(std::move(state)),
      last_confirmed_opid_(std::move(last_confirmed_opid)),
      dup_info_(std::move(dup_info)) {}

void DuplicationOp::NewReplicateMsg(unique_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(consensus::OperationType::DUPLICATE_OP);
  consensus::DuplicateRequestPB* duplicate_request = (*replicate_msg)->mutable_duplicate_request();
  duplicate_request->mutable_op_id()->CopyFrom(last_confirmed_opid_);
  duplicate_request->mutable_dup_info()->CopyFrom(dup_info_);
}

Status DuplicationOp::Prepare() {
  return Status::OK();
}

Status DuplicationOp::Start() {
  DCHECK(!state_->has_timestamp());
  DCHECK(state_->consensus_round()->replicate_msg()->has_timestamp());
  state_->set_timestamp(Timestamp(state_->consensus_round()->replicate_msg()->timestamp()));
  TRACE("START. Timestamp: $0", clock::HybridClock::GetPhysicalValueMicros(state_->timestamp()));
  return Status::OK();
}

Status DuplicationOp::Apply(CommitMsg** commit_msg) {
  TRACE("APPLY DUPLICATION: Starting");
  // Do nothing to local kudu engine.

  const consensus::DuplicateRequestPB* request = state_->request();
  state_->tablet_replica()->set_duplicator_last_committed_opid(request->op_id());
  // Create the Commit message
  *commit_msg = google::protobuf::Arena::CreateMessage<CommitMsg>(state_->pb_arena());
  (*commit_msg)->set_op_type(consensus::OperationType::DUPLICATE_OP);

  // Altered tablets should be included in the next tserver heartbeat so that
  // clients waiting on IsAlterTableDone() are unblocked promptly.
  state_->tablet_replica()->MarkTabletDirty("DuplicationOp finished");
  return Status::OK();
}

void DuplicationOp::Finish(OpResult result) {
  if (PREDICT_FALSE(result == Op::ABORTED)) {
    TRACE("DuplicationOpCallback: op aborted");
    state()->Finish();
    return;
  }
  DCHECK_EQ(result, Op::APPLIED);
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("DuplicationOpCallback: making duplication visible");
  state()->Finish();
}

string DuplicationOp::ToString() const {
  return Substitute("DuplicationOp [state=$0]", state_->ToString());
}

}  // namespace tablet
}  // namespace kudu
