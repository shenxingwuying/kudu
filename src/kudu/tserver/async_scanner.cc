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

#include "kudu/tserver/async_scanner.h"
#include <kudu/tserver/service_coprocess.h>

namespace kudu {
namespace tserver {

void AsyncScanner::OnStagedEventDriven(seda::AsyncContext* context) {
  TRACE(Substitute("AsyncScanner received action: $0", seda::Action2String(context->action)));
  switch (context->action) {
    case seda::Action::kTServerNewScan: {
      OnNewScan(context);
      break;
    }
    case seda::Action::kTServerContinueScan: {
      OnContinueScan(context);
      break;
    }
    default: {
      VLOG(0) << "unsupported action: " << seda::Action2String(context->action);
      break;
    }
  }
}

void AsyncScanner::OnNewScan(seda::AsyncContext* context) {
  std::unique_ptr<seda::AsyncContext> context_ptr(context);
  ScanContext* ctx = reinterpret_cast<ScanContext*>(context);

  ScanResultCopier collector(GetMaxBatchSizeBytesHint(ctx->req));
  bool has_more_results = false;
  TabletServerErrorPB::Code error_code = TabletServerErrorPB::UNKNOWN_ERROR;

  if (!CheckTabletServerNotQuiescingOrRespond(ctx->server, ctx->resp, ctx->rpc_context)) {
    return;
  }
  const NewScanRequestPB& scan_pb = ctx->req->new_scan_request();
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(ctx->server->tablet_manager(),
                                           scan_pb.tablet_id(),
                                           ctx->resp,
                                           ctx->rpc_context,
                                           &replica)) {
    return;
  }
  string scanner_id;
  Timestamp scan_timestamp;
  Status s = ctx->service->HandleNewScanRequest(replica.get(),
                                                ctx->req,
                                                ctx->rpc_context,
                                                &collector,
                                                &scanner_id,
                                                &scan_timestamp,
                                                &has_more_results,
                                                &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(ctx->resp->mutable_error(), s, error_code, ctx->rpc_context);
    return;
  }

  // Only set the scanner id if we have more results.
  if (has_more_results) {
    ctx->resp->set_scanner_id(scanner_id);
  }
  if (scan_timestamp != Timestamp::kInvalidTimestamp) {
    ctx->resp->set_snap_timestamp(scan_timestamp.ToUint64());
  }

  collector.SetupResponse(ctx->rpc_context, ctx->resp);
  ctx->resp->set_has_more_results(has_more_results);
  ctx->resp->set_propagated_timestamp(ctx->server->clock()->Now().ToUint64());

  SetResourceMetrics(
      ctx->rpc_context, collector.cpu_times(), ctx->resp->mutable_resource_metrics());
  ctx->rpc_context->RespondSuccess();
}

void AsyncScanner::OnContinueScan(seda::AsyncContext* context) {
  std::unique_ptr<seda::AsyncContext> context_ptr(context);
  ScanContext* ctx = reinterpret_cast<ScanContext*>(context);

  ScanResultCopier collector(GetMaxBatchSizeBytesHint(ctx->req));
  bool has_more_results = false;
  TabletServerErrorPB::Code error_code = TabletServerErrorPB::UNKNOWN_ERROR;

  Status s = ctx->service->HandleContinueScanRequest(
      ctx->req, ctx->rpc_context, &collector, &has_more_results, &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(ctx->resp->mutable_error(), s, error_code, ctx->rpc_context);
    return;
  }

  collector.SetupResponse(ctx->rpc_context, ctx->resp);
  ctx->resp->set_has_more_results(has_more_results);
  ctx->resp->set_propagated_timestamp(ctx->server->clock()->Now().ToUint64());

  SetResourceMetrics(
      ctx->rpc_context, collector.cpu_times(), ctx->resp->mutable_resource_metrics());
  ctx->rpc_context->RespondSuccess();
}

}  // namespace tserver
}  // namespace kudu