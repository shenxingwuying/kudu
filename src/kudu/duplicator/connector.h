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
// The file is define a Common Connector interface api, 
// if user need to duplicate data to another storage, 
// user should extend the interface just as kafka/kafka_connector.h.

#pragma once

#include <memory>
#include <vector>
#include <string>

#include "kudu/common/schema.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/row_op.h"
#include "kudu/util/status.h"
#include "google/protobuf/arena.h"
#include "kudu/util/memory/arena.h"

namespace kudu {
namespace duplicator {

using std::string;

struct DuplicateMsg {
  const std::shared_ptr<tablet::WriteOpState> op_state;
  const tablet::RowOp* row_op;
  std::shared_ptr<Schema> schema_ptr;
  std::string table_name;

  DuplicateMsg(const std::shared_ptr<tablet::WriteOpState>& _op_state,
               const tablet::RowOp* _row_op,
               const std::shared_ptr<Schema>& _schema_ptr,
               const std::string& _table_name)
      : op_state(_op_state), row_op(_row_op),
        schema_ptr(_schema_ptr), table_name(_table_name) {

      }

  DuplicateMsg(const DuplicateMsg& /**/) = delete;
  void operator=(const DuplicateMsg& /**/) = delete;
};

class Connector {
 public:
  virtual ~Connector() = default;
  virtual Status InitPrepare() = 0;
  virtual Status Init() = 0;
  virtual Status WriteBatch(const std::vector<std::shared_ptr<DuplicateMsg>>& msgs) = 0;
  // For test basic client api
  virtual Status TestBasicClientApi(const std::string& /* msg */) { return Status::OK(); }
};

/**
 * Local Connector, such as mongodb's hidden replica.
 * Also for some situation's backup.
 * If use KuduConnector, that's just added another replica,
 * but non-voter.
 * If use kv Connector, eg: leveldb/rocksdb, it can serving some random query
 * more effient
 */
class LocalConnector : public Connector {
 public:
  LocalConnector() : Connector() {}
  virtual ~LocalConnector() = default;
};

class RemoteConnector : public Connector {
 public:
  RemoteConnector() : Connector() {}
  virtual ~RemoteConnector() = default;
};

/**
 * KuduLocalConnector's special, it can be implemented directly.
 * It equals Kudu has 4 replicas and 1 is learner not participant in voting.
 * So the class will not use.
 * The class just is a represent for LocalConnector.
 */
class KuduLocalConnector : public LocalConnector {
 public:
  KuduLocalConnector() : LocalConnector() {}
  virtual ~KuduLocalConnector() = default;
  virtual Status Init() override {
    LOG(INFO) << "KuduLocalConnector Init()";
    return Status::OK();
  }
  virtual Status WriteBatch(const std::vector<std::shared_ptr<DuplicateMsg>>& /* msgs */) override {
    LOG(INFO) << "KuduLocalConnector WriteBatch(), just stand for it. not use";
    return Status::OK();
  }
};

}  // namespace duplicator
}  // namespace kudu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
