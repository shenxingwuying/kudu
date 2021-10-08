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
// The file is define a Duplicator interface api, 
// if user need to duplicate data to another storage, 
// user should implement the interface just like kafka below.

#include "kudu/duplicator/duplicator.h"

#include <deque>
#include <functional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/opid_util.h"
#include "kudu/duplicator/kafka/kafka_connector.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/monotime.h"
#include "kudu/util/threadpool.h"

namespace kudu {
class Schema;
namespace tablet {
struct RowOp;
}  // namespace tablet
}  // namespace kudu

DEFINE_string(downstream_type, "kafka", "");
DEFINE_int64(duplicate_pool_timeout_s, 10, "");

static bool ValidateDownstreamFlag(const char* flagname, const std::string& value) {
  if (value == "kafka" || value == "local_kudu") {
    return true;
  }
  LOG(ERROR) << "duplication downstream_type should be: kafka, local_kudu";
  return false;
}
DEFINE_validator(downstream_type, &ValidateDownstreamFlag);

namespace kudu {
namespace duplicator {

Duplicator::Duplicator(ThreadPool* thread_pool, const std::string& table_name)
    : duplicate_pool_(thread_pool),
      table_name_(table_name),
      last_confirmed_opid_(consensus::MinimumOpId()),
      stopped_(true),
      queue_(8192) {
  // #ifdef ENABLE_DUPLICATION

  if (FLAGS_downstream_type == "kafka") {
    connector_ = kafka::KafkaConnector<cppkafka::Producer>::GetInstance();
  } else {
    // Not Supported
    connector_ = nullptr;
  }
  // #else
  // Not Support
  //    connector_ = nullptr;
  //#endif
}
Duplicator::~Duplicator() {
  // @TODO(duyuqi)
  // no release connector, for its a shared object
  connector_ = nullptr;
}

Status Duplicator::Init() {
  stopped_ = false;
  connector_->Init();
  duplicate_pool_token_ = duplicate_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL
                                                    // TODO metric_entity for duplicate_pool_
  );
  return Status::OK();
}

Status Duplicator::Shutdown() {
  stopped_ = true;
  // TODO(duyuqi), duplicate_pool_token_ should not shutdown for its shared
  // if (duplicate_pool_token_ != nullptr) {
  //   duplicate_pool_token_->Shutdown();
  // }
  // TODO deal queue data
  return Status::OK();
}

void Duplicator::Duplicate(const std::shared_ptr<tablet::WriteOpState>& write_op_state_ptr, 
                           const tablet::RowOp* row_op, const std::shared_ptr<Schema>& schema) {
  if (stopped_) {
    return;
  }
  
  std::shared_ptr<DuplicateMsg> msg = std::make_shared<DuplicateMsg>(
    write_op_state_ptr,
    row_op, schema, 
    table_name_);
  queue_.Put(msg);
  WARN_NOT_OK(duplicate_pool_token_->Submit(
      [this]() { this->Apply(); }),
      "duplicator apply failed");
}

void Duplicator::Apply() {
  std::vector<std::shared_ptr<DuplicateMsg>> msgs;
  queue_.BlockingDrainTo(&msgs, MonoTime::Now() + MonoDelta::FromSeconds(30));
  if (!msgs.empty()) {
    LOG(INFO) << "msg size: " << msgs.size();
    // sync call
    Status s = connector_->WriteBatch(msgs);
    if (s.ok()) {
      auto rit = msgs.rbegin();
      {
        std::lock_guard<std::mutex> l(mutex_);
        last_confirmed_opid_ = (*rit)->op_state->op_id();
      }
      LOG(INFO) << "duplicator confirmed opid: (" << last_confirmed_opid_.term() << "," 
        << last_confirmed_opid_.index() << ")";
    }
  }
}

}  // namespace duplicator
}  // namespace kudu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */