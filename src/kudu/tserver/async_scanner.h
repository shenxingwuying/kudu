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

#include <kudu/util/seda.h>
#include <memory>
#include "kudu/common/schema.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_service.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/rpc/rpc_context.h"

namespace kudu {
namespace tserver {

using kudu::rpc::RpcContext;

struct ScanContext : seda::AsyncContext {
  const ScanRequestPB* req;
  ScanResponsePB* resp;
  RpcContext* rpc_context;
  TabletServer* server;
  tserver::TabletServiceImpl* service;

  ScanContext(seda::Action action, std::shared_ptr<seda::AsyncClient> client)
      : seda::AsyncContext(action, std::move(client)) {}
};

class AsyncScanner : public seda::AsyncClient {
 public:
  void OnStagedEventDriven(seda::AsyncContext* context) override;

  void OnNewScan(seda::AsyncContext* context);
  void OnContinueScan(seda::AsyncContext* context);
};


}  // namespace tserver
}  // namespace kudu
