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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include <boost/optional/optional.hpp>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/status.h"

namespace kudu {

class rw_semaphore;

namespace tablet {

class OperationResultPB;
class TabletReplica;

// Op Context for the Duplication operation. Duplication Op is
// to duriable progressing of duplication.
class DuplicationOpState : public OpState {
 public:
  ~DuplicationOpState() {}

  explicit DuplicationOpState(TabletReplica* tablet_replica,
                              consensus::DuplicateRequestPB request,
                              consensus::DuplicateResponsePB response)
      : OpState(tablet_replica), request_(std::move(request)), response_(std::move(response)) {}

  // Note: request_ and response_ are set to null after this method returns.
  void Finish() {}

  const consensus::DuplicateRequestPB* request() const override { return &request_; }

  // Returns the response PB associated with this op, or NULL.
  // This will only return a non-null object for leader-side ops.
  consensus::DuplicateResponsePB* response() const override {
    return const_cast<consensus::DuplicateResponsePB*>(&response_);
  }

  // Sets the fact that the alter had an error.
  void SetError(const Status& s);

  boost::optional<OperationResultPB> error() const { return error_; }

  std::string ToString() const override;

 private:
  DISALLOW_COPY_AND_ASSIGN(DuplicationOpState);
  const consensus::DuplicateRequestPB request_;
  consensus::DuplicateResponsePB response_;

  // The error result of this alter schema op. May be empty if the
  // op hasn't been applied or if the alter succeeded.
  boost::optional<OperationResultPB> error_;
};

// Executes the alter schema op.
class DuplicationOp : public Op {
 public:
  DuplicationOp(std::unique_ptr<DuplicationOpState> state,
                consensus::DriverType type,
                consensus::OpId last_confirmed_opid,
                consensus::DuplicationInfoPB dup_info);

  DuplicationOpState* state() override { return state_.get(); }
  const DuplicationOpState* state() const override { return state_.get(); }

  void NewReplicateMsg(std::unique_ptr<consensus::ReplicateMsg>* replicate_msg) override;

  // Executes a Prepare for the alter schema op.
  Status Prepare() override;

  // Starts the DuplicationOp by assigning it a timestamp.
  Status Start() override;

  // Executes an Apply for the alter schema op
  Status Apply(consensus::CommitMsg** commit_msg) override;

  Status Duplicate() override {
    // Do nothing.
    return Status::NotSupported("DuplicationOp no support duplicate.");
  }

  // Actually commits the op.
  void Finish(OpResult result) override;

  std::string ToString() const override;

 private:
  std::unique_ptr<DuplicationOpState> state_;
  consensus::OpId last_confirmed_opid_;
  consensus::DuplicationInfoPB dup_info_;
  DISALLOW_COPY_AND_ASSIGN(DuplicationOp);
};

}  // namespace tablet
}  // namespace kudu
