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

#include <atomic>
#include <deque>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/duplicator/connector_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/monotime.h"
#include "kudu/util/threadpool.h"

namespace kudu {
class Schema;
}  // namespace kudu

using strings::Substitute;

DEFINE_int64(duplicator_max_queue_size, 1024,
            "the duplicator's max queue size, if excess the limit, duplication would downgrade "
            "from realtime mode to wal mode, duplication progress would be slow.");
DEFINE_int32(queue_drain_timeout_ms, 5000,
             "if queue no rows, wait queue_drain_timeout_ms ms at most ");

using DuplicationMode = kudu::tablet::Tablet::DuplicationMode;

namespace kudu {
namespace duplicator {

Duplicator::Duplicator(ThreadPool* duplicate_pool,
                       tablet::TabletReplica* tablet_replica)
    : duplicate_pool_(duplicate_pool),
      tablet_replica_(tablet_replica),
      last_confirmed_opid_(consensus::MinimumOpId()),
      stopped_(true),
      queue_(FLAGS_duplicator_max_queue_size) {
  connector_manager_ = tablet_replica->connector_manager();
}

Duplicator::~Duplicator() {}

Status Duplicator::Init(const ConnectorOptions& options) {
  options_ = options;
  connector_ = connector_manager_->GetOrNewConnector(options);
  RETURN_NOT_OK(connector_->Init(options));
  duplicate_pool_token_ =
      duplicate_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL
                                // TODO(duyuqi) metric_entity for duplicate_pool_
      );
  stopped_ = false;
  VLOG(0) << Substitute("duplicator init finish, ", ToString());
  return Status::OK();
}

Status Duplicator::Shutdown() {
  stopped_ = true;
  if (duplicate_pool_token_) {
    duplicate_pool_token_->Shutdown();
    duplicate_pool_token_.reset();
  }
  return Status::OK();
}

Status Duplicator::Duplicate(tablet::WriteOpState* write_op_state,
                             tablet::Tablet::DuplicationMode /*expect_mode*/) {
  if (stopped_) {
    return Status::IllegalState("duplicator is stopped");
  }
  std::unique_ptr<DuplicateMsg> msg = std::make_unique<DuplicateMsg>(
      write_op_state, tablet_replica_->tablet_metadata()->table_name());
  consensus::OpId op_id = write_op_state->op_id();

  RETURN_NOT_OK_LOG(msg->ParseKafkaRecord(), ERROR, "parse kafka record failed");
  switch (queue_.Put(std::move(msg))) {
    case QueueStatus::QUEUE_SUCCESS:
      break;
    case QueueStatus::QUEUE_FULL: {
      // If QUEUE_FULL, should switch to inc wal duplication mode.
    } break;
    case QueueStatus::QUEUE_SHUTDOWN:
      return Status::IllegalState("duplicator' queue state: shutdown");
    default:
      return Status::IllegalState("duplicator' queue state: unknown");
  }

  return duplicate_pool_token_->Submit([this]() { this->Apply(); });
}

void Duplicator::Apply() {
  std::vector<std::unique_ptr<DuplicateMsg>> msgs;
  WARN_NOT_OK(
      queue_.BlockingDrainTo(
          &msgs, MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_queue_drain_timeout_ms)),
      "queue BlockingDrainTo error");

  if (!msgs.empty()) {
    // sync call
    Status s = Status::OK();
    while (!(s = connector_->WriteBatch(options_.name, msgs)).ok()) {
      KLOG_EVERY_N(WARNING, 100) << Substitute(
          "WriteBatch destination storage system error, retry it, status $0, $1",
          s.ToString(),
          ToString());
      // @TODO(duyuqi). We'd better add some backoff time.
      SleepFor(MonoDelta::FromMilliseconds(20));
    }
    auto rit = msgs.rbegin();
    {
      std::lock_guard<std::mutex> l(mutex_);
      last_confirmed_opid_ = (*rit)->op_state()->op_id();
    }
    VLOG(1) << ToString();
  }
}

string Duplicator::ToString() const {
  return Substitute("duplicator info, confirmed opid: $0, tablet id: $1, peer uuid: $2",
                    last_confirmed_opid().ShortDebugString(),
                    tablet_replica_->tablet_id(),
                    tablet_replica_->permanent_uuid());
}
}  // namespace duplicator
}  // namespace kudu
