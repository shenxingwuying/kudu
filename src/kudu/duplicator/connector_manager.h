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
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <kudu/duplicator/connector.h>
#include "kudu/consensus/metadata.pb.h"
#include "kudu/duplicator/kafka/kafka_connector.h"
#include "kudu/gutil/map-util.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/random_util.h"

DECLARE_int32(duplication_connectors_count);

namespace kudu {
namespace duplicator {

// ConnectorManager is a singleton, it includes only one Connector.
// In sensorsdata.com, one Connector conresponding to the only kafka cluster.
// A Connector is responsible to write ops to thirdparty destination storage system.
// <System Type, Uri> are an unique key to mark a Connector, in ConnectorManager
// A kind Connector exist only one.
class ConnectorManager {
 public:
  ConnectorManager()
      : connectors_(FLAGS_duplication_connectors_count), random_(GetRandomSeed32()) {}
  ~ConnectorManager() {}

  Connector* GetOrNewConnector(const ConnectorOptions& options) {
    int index = static_cast<int>(random_.Uniform(FLAGS_duplication_connectors_count));
    std::unique_lock<Mutex> l(lock_);
    if (connectors_[index] != nullptr) {
      bool is_kerberos_equal =
          connectors_[index]->connector_options().IsKerberosOptionsEqual(options);
      if (is_kerberos_equal) {
        return GetConnector(options, index);
      }
      // TODO(duyuqi)
      // Because connector manager is more and more complex, so it should
      // redesign to avoid bugs. Now I think adding registering connectors
      // before using it at next patch.
      // This is for change kerberos options.
      connectors_[index].reset();
    }
    connectors_[index] = NewConnector(options);
    return connectors_[index].get();
  }

  // Only for test, unsafe, should control by coders.
  void TestDestructAll() {
    for (int i = 0 ; i < FLAGS_duplication_connectors_count; ++i) {
      connectors_[i].reset();
    }
  }

 private:
  Connector* GetConnector(const ConnectorOptions& options, int index) {
    string exist_normalized_uri;
    string* normal_uri_p = FindOrNull(normalized_uri_cache_, connectors_[index]->Uri());
    if (normal_uri_p) {
      exist_normalized_uri = *normal_uri_p;
    } else {
      if (!consensus::NormalizeUri(
               connectors_[index]->Type(), connectors_[index]->Uri(), &exist_normalized_uri)
               .ok()) {
        return nullptr;
      }
      InsertOrDie(&normalized_uri_cache_, connectors_[index]->Uri(), exist_normalized_uri);
    }
    string expected_normalized_uri;
    normal_uri_p = FindOrNull(normalized_uri_cache_, options.uri);
    if (normal_uri_p) {
      expected_normalized_uri = *normal_uri_p;
    } else {
      if (!consensus::NormalizeUri(options.type, options.uri, &expected_normalized_uri).ok()) {
        return nullptr;
      }
      InsertOrDie(&normalized_uri_cache_, options.uri, expected_normalized_uri);
    }
    if (connectors_[index]->Type() == options.type &&
        expected_normalized_uri == exist_normalized_uri) {
      return connectors_[index].get();
    }
    LOG(ERROR) << strings::Substitute(
        "invalid connector option, kudu has exist a connector: $0, "
        "request connector options: $1",
        connectors_[index]->connector_options().ToString(),
        options.ToString());
    return nullptr;
  }

  static std::unique_ptr<Connector> NewConnector(const ConnectorOptions& options) {
    if (options.type == consensus::DownstreamType::KAFKA) {
      std::unique_ptr<Connector> connector =
          std::make_unique<kafka::KafkaConnector>(options);
      if (!connector->Init(options).ok()) {
        connector.reset();
        CHECK(!connector);
      }
      return connector;
    }
    LOG(FATAL) << strings::Substitute("type: $0 is not kafka, support only kafka now.",
                                      consensus::DownstreamType_Name(options.type));
    return nullptr;
  }

  // protect connectors_ and normalized_uri_cache_.
  Mutex lock_;
  // Duplication supports only one type Connector: a kafka cluster's connector.
  std::vector<std::unique_ptr<Connector>> connectors_;
  // non-normalized map to normalized uri.
  std::unordered_map<string, string> normalized_uri_cache_;
  Random random_;
};

}  // namespace duplicator
}  // namespace kudu
