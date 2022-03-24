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
// The file define a Duplicator, it contains common logic codes, 
// User shoud pick Connector what you want.

#pragma once

#include <memory>

#include "kudu/consensus/opid.pb.h"
#include "kudu/duplicator/connector.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;
class ThreadPool;
class ThreadPoolToken;
namespace tablet {
class Tablet;
struct RowOp;
}  // namespace tablet

namespace duplicator {

class Duplicator {
public:
    Duplicator(ThreadPool* thread_pool, const std::string& table_name);
    ~Duplicator();
    Duplicator(const Duplicator& /* dup */) = delete;
    void operator=(const Duplicator& /* dup */) = delete;

    Status Init();
    Status Shutdown();

    void Duplicate(const std::shared_ptr<tablet::WriteOpState>& op_state_ptr, 
                   const tablet::RowOp* row_op, const std::shared_ptr<Schema>& schema);
    void Apply();
    const consensus::OpId last_confirmed_opid() const {
        std::lock_guard<std::mutex> l(mutex_);
        return last_confirmed_opid_;
    }
private:
    ThreadPool* duplicate_pool_;
    // std::shared_ptr<tablet::Tablet> tablet_;
    const std::string table_name_;
    consensus::OpId last_confirmed_opid_;
    mutable std::mutex mutex_;
    bool stopped_;
    // TODO use pointer to avoid copy
    BlockingQueue<std::shared_ptr<DuplicateMsg>> queue_;

    // Singleton, eg KafkaConnector
    Connector* connector_;
    std::unique_ptr<ThreadPoolToken> duplicate_pool_token_;
};

} // namespace duplicator
} // namespace kudu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
