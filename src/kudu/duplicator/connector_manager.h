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
// the codes based on some ningw's internal work, and then refact.
//

#pragma once

#include <algorithm>
#include <cctype>
#include <map>
#include <string>

#include <kudu/duplicator/connector.h>
#include "kudu/consensus/metadata.pb.h"
#include "kudu/duplicator/kafka/kafka_connector.h"

namespace kudu {
namespace duplicator {

// ConnectorManager is a singleton, it includes only one Connector.
// In sensorsdata.com, one Connector conresponding to the only kafka cluster.
// A Connector is responsible to write ops to thirdparty destination storage system.
// <System Type, Uri> are an unique key to mark a Connector, in ConnectorManager
// A kind Connector exist only one.
class ConnectorManager {
 public:
  ConnectorManager() {}
  ~ConnectorManager() {
    delete connector_;
    connector_ = nullptr;
  }

  Connector* GetOrNewConnector(const ConnectorOptions& options) {
    if (connector_ != nullptr) {
      return GetConnector(options);
    }

    std::unique_lock<Mutex> l(lock_);
    if (connector_ != nullptr) {
      return GetConnector(options);
    }
    connector_ = NewConnector(options);
    return connector_;
  }

  // Only for test, unsafe, should control by coders.
  void TestDestructAll() {
    delete connector_;
    connector_ = nullptr;
  }

 private:
  Connector* GetConnector(const ConnectorOptions& options) {
    CHECK(connector_->Type() == options.type && connector_->Uri() == options.uri);
    return connector_;
  }

  static Connector* NewConnector(const ConnectorOptions& options) {
    if (options.type == consensus::DownstreamType::KAFKA) {
      Connector* connector = new kafka::KafkaConnector(options);
      if (!connector->Init(options).ok()) {
        delete connector;
        return nullptr;
      }
      return connector;
    }
    LOG(FATAL) << strings::Substitute("type: $0 is not kafka, support only kafka now.",
                                      consensus::DownstreamType_Name(options.type));
    return nullptr;
  }

  Mutex lock_;
  // Duplication supports only one Connector.
  Connector* connector_;
};

}  // namespace duplicator
}  // namespace kudu
