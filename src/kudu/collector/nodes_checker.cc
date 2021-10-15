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

#include "kudu/collector/nodes_checker.h"

#include <cstdint>
#include <functional>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <rapidjson/document.h>

#include "kudu/collector/collector_util.h"
#include "kudu/collector/prometheus_reporter.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"

DECLARE_string(collector_cluster_name);
DECLARE_string(collector_master_addrs);
DECLARE_uint32(collector_metrics_collect_interval_sec);
DECLARE_uint32(collector_timeout_sec);
DECLARE_uint32(collector_warn_threshold_ms);
DECLARE_string(ksyncer_uuid);

using rapidjson::Value;
using std::string;
using std::vector;
using strings::Substitute;
using kudu::cluster_summary::ServerHealth;
using kudu::cluster_summary::HealthCheckResult;

namespace kudu {
namespace collector {

const char NodesChecker::kMaster[] = "master";
const char NodesChecker::kTserver[] = "tserver";

NodesChecker::NodesChecker()
  : initialized_(false),
    stop_background_threads_latch_(1) {
}

NodesChecker::~NodesChecker() {
  Shutdown();
}

Status NodesChecker::Init() {
  CHECK(!initialized_);

  RETURN_NOT_OK(UpdateNodes());
  CHECK(!master_http_addrs_.empty());

  initialized_ = true;
  return Status::OK();
}

Status NodesChecker::Start() {
  CHECK(initialized_);

  RETURN_NOT_OK(StartNodesCheckerThread());

  return Status::OK();
}

void NodesChecker::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    stop_background_threads_latch_.CountDown();
    if (nodes_checker_thread_) {
      nodes_checker_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

string NodesChecker::ToString() {
  return "NodesChecker";
}

vector<string> NodesChecker::GetMasters() {
  shared_lock<RWMutex> l(nodes_lock_);
  return master_http_addrs_;
}

vector<string> NodesChecker::GetTServers() {
  shared_lock<RWMutex> l(nodes_lock_);
  return tserver_http_addrs_;
}

string NodesChecker::GetFirstMaster() {
  shared_lock<RWMutex> l(nodes_lock_);
  CHECK(!master_http_addrs_.empty());
  return master_http_addrs_[0];
}

string NodesChecker::GetFirstTServer() {
  shared_lock<RWMutex> l(nodes_lock_);
  CHECK(!tserver_http_addrs_.empty());
  return tserver_http_addrs_[0];
}

Status NodesChecker::StartNodesCheckerThread() {
  return Thread::Create("collector", "nodes-checker",
                        [this]() { this->NodesCheckerThread(); },
                        &nodes_checker_thread_);
}

void NodesChecker::NodesCheckerThread() {
  MonoTime check_time;
  do {
    check_time = MonoTime::Now();
    UpdateAndCheckNodes();
    check_time += MonoDelta::FromSeconds(FLAGS_collector_metrics_collect_interval_sec);
  } while (!RunOnceMode() && !stop_background_threads_latch_.WaitUntil(check_time));
  LOG(INFO) << "NodesCheckerThread exit";
}

void NodesChecker::UpdateAndCheckNodes() {
  LOG(INFO) << "Start to UpdateAndCheckNodes";
  MonoTime start(MonoTime::Now());
  scoped_refptr<Trace> trace(new Trace);
  ADOPT_TRACE(trace.get());
  WARN_NOT_OK(UpdateNodes(), "Unable to update nodes");
  WARN_NOT_OK(CheckNodes(), "Unable to check nodes");
  int64_t elapsed_ms = (MonoTime::Now() - start).ToMilliseconds();
  if (elapsed_ms > FLAGS_collector_warn_threshold_ms) {
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << "Trace:" << std::endl
                   << Trace::CurrentTrace()->DumpToString();
    }
  }
}

Status NodesChecker::UpdateNodes() {
  RETURN_NOT_OK(UpdateServers(kMaster));
  RETURN_NOT_OK(UpdateServers(kTserver));
  return Status::OK();
}

Status NodesChecker::UpdateServers(const string& role) {
  DCHECK(role == kTserver || role == kMaster);
  vector<string> args = {
    role,
    "list",
    FLAGS_collector_master_addrs,
    "-columns=http-addresses,uuid",
    "-format=json",
    Substitute("-timeout_ms=$0", FLAGS_collector_timeout_sec*1000)
  };
  string tool_stdout;
  string tool_stderr;
  RETURN_NOT_OK_PREPEND(tools::RunKuduTool(args, &tool_stdout, &tool_stderr),
                        Substitute("out: $0, err: $1", tool_stdout, tool_stderr));
  TRACE(Substitute("'$0 list' done", role));

  JsonReader r(tool_stdout);
  RETURN_NOT_OK(r.Init());
  vector<const Value*> servers;
  CHECK_OK(r.ExtractObjectArray(r.root(), nullptr, &servers));
  vector<string> server_http_addrs;
  for (const Value* server : servers) {
    string uuid;
    CHECK_OK(r.ExtractString(server, "uuid", &uuid));
    if (uuid == FLAGS_ksyncer_uuid) {
      continue;
    }
    string http_address;
    CHECK_OK(r.ExtractString(server, "http-addresses", &http_address));
    server_http_addrs.emplace_back(http_address);
  }
  TRACE(Substitute("Result parsed, nodes count $0", server_http_addrs.size()));

  if (role == kTserver) {
    std::lock_guard<RWMutex> l(nodes_lock_);
    tserver_http_addrs_.swap(server_http_addrs);
  } else {
    std::lock_guard<RWMutex> l(nodes_lock_);
    master_http_addrs_.swap(server_http_addrs);
  }
  TRACE("Nodes updated");

  return Status::OK();
}

Status NodesChecker::CheckNodes() {
  vector<string> args = {
    "cluster",
    "ksck",
    FLAGS_collector_master_addrs,
    "-consensus=false",
    "-ksck_format=json_compact",
    "-color=never",
    "-sections=MASTER_SUMMARIES,TSERVER_SUMMARIES,TABLE_SUMMARIES,TOTAL_COUNT",
    Substitute("-timeout_ms=$0", FLAGS_collector_timeout_sec*1000)
  };
  string tool_stdout;
  string tool_stderr;
  WARN_NOT_OK(tools::RunKuduTool(args, &tool_stdout, &tool_stderr),
              Substitute("out: $0, err: $1", tool_stdout, tool_stderr));

  TRACE("'cluster ksck' done");

  RETURN_NOT_OK(ReportNodesMetrics(tool_stdout));
  return Status::OK();
}

Status NodesChecker::ReportNodesMetrics(const string& data) {
  JsonReader r(data);
  RETURN_NOT_OK(r.Init());
  const Value* ksck;
  CHECK_OK(r.ExtractObject(r.root(), nullptr, &ksck));

  // Maters health info.
  vector<const Value*> masters;
  CHECK_OK(r.ExtractObjectArray(ksck, "master_summaries", &masters));
  auto& master_health = prometheus::BuildGauge()
      .Name("kudu_master_health")
      .Help("Kudu master health status "
            "(0: HEALTHY, 1: UNAUTHORIZED, 2: UNAVAILABLE, 3: WRONG_SERVER_UUID)")
      .Register(PrometheusReporter::instance()->registry());
  for (const Value* master : masters) {
    string address;
    CHECK_OK(r.ExtractString(master, "address", &address));
    string health;
    CHECK_OK(r.ExtractString(master, "health", &health));
    auto& metric = master_health.Add({{"master", ExtractHostName(address)}});
    metric.Set(static_cast<int64_t>(ExtractServerHealthStatus(health)));
  }
  TRACE(Substitute("Maters health info reported, count $0", masters.size()));

  // Tservers health info.
  vector<const Value*> tservers;
  Status s = r.ExtractObjectArray(ksck, "tserver_summaries", &tservers);
  CHECK(s.ok() || s.IsNotFound());
  auto& tserver_health = prometheus::BuildGauge()
      .Name("kudu_tserver_health")
      .Help("Kudu tserver health status "
            "(0: HEALTHY, 1: UNAUTHORIZED, 2: UNAVAILABLE, 3: WRONG_SERVER_UUID)")
      .Register(PrometheusReporter::instance()->registry());
  if (s.ok()) {
    for (const Value* tserver : tservers) {
      string address;
      CHECK_OK(r.ExtractString(tserver, "address", &address));
      string health;
      CHECK_OK(r.ExtractString(tserver, "health", &health));
      auto& metric = tserver_health.Add({{"tserver", ExtractHostName(address)}});
      metric.Set(static_cast<int64_t>(ExtractServerHealthStatus(health)));
    }
    TRACE(Substitute("Tservers health info reported, count $0", tservers.size()));
  }

  // Tables health info.
  uint32_t health_table_count = 0;
  vector<const Value*> tables;
  s = r.ExtractObjectArray(ksck, "table_summaries", &tables);
  CHECK(s.ok() || s.IsNotFound());
  auto& table_health = prometheus::BuildGauge()
      .Name("kudu_table_health")
      .Help("Kudu table health status "
            "(0: HEALTHY, 1: RECOVERING, 2: UNDER_REPLICATED, "
            "3: UNAVAILABLE, 4: CONSENSUS_MISMATCH)")
      .Register(PrometheusReporter::instance()->registry());
  if (s.ok()) {
    for (const Value* table : tables) {
      string name;
      CHECK_OK(r.ExtractString(table, "name", &name));
      string health;
      CHECK_OK(r.ExtractString(table, "health", &health));
      HealthCheckResult health_status = ExtractTableHealthStatus(health);

      auto& metric = table_health.Add({{"table", name}});
      metric.Set(static_cast<int64_t>(health_status));

      if (health_status == HealthCheckResult::HEALTHY) {
        health_table_count += 1;
      }
    }
    TRACE(Substitute("Tables health info reported, count $0", tables.size()));
  }

  // Healthy table proportion.
  if (!tables.empty()) {
    auto& table_health = prometheus::BuildGauge()
        .Name("kudu_healthy_table_proportion")
        .Help("Kudu health table proportion, range in [0.0, 1.0]")
        .Register(PrometheusReporter::instance()->registry());

    auto& metric = table_health.Add({});
    metric.Set(static_cast<double>(health_table_count) / tables.size());

    TRACE("Healthy table ratio reported");
  }

  // Count summaries.
  vector<const Value*> count_summaries;
  CHECK_OK(r.ExtractObjectArray(ksck, "count_summaries", &count_summaries));
  auto& instance_count = prometheus::BuildGauge()
      .Name("kudu_instance_count")
      .Help("Kudu different role instance count")
      .Register(PrometheusReporter::instance()->registry());
  for (const Value* count_summarie : count_summaries) {
    // TODO(yingchun): should auto iterate items.
    static const vector<string>
        count_names({"masters", "tservers", "tables", "tablets", "replicas"});
    for (const auto& name : count_names) {
      int64_t count;
      CHECK_OK(r.ExtractInt64(count_summarie, name.c_str(), &count));

      auto& metric = instance_count.Add({{"role", name}});
      metric.Set(count);
    }
  }
  TRACE("Count summaries reported");

  return Status::OK();
}

ServerHealth NodesChecker::ExtractServerHealthStatus(const string& health) {
  if (health == "HEALTHY") return ServerHealth::HEALTHY;
  if (health == "UNAUTHORIZED") return ServerHealth::UNAUTHORIZED;
  if (health == "UNAVAILABLE") return ServerHealth::UNAVAILABLE;
  if (health == "WRONG_SERVER_UUID") return ServerHealth::WRONG_SERVER_UUID;
  CHECK(false) << "Unknown server health: " << health;
  __builtin_unreachable();
}

HealthCheckResult NodesChecker::ExtractTableHealthStatus(const string& health) {
  if (health == "HEALTHY") return HealthCheckResult::HEALTHY;
  if (health == "RECOVERING") return HealthCheckResult::RECOVERING;
  if (health == "UNDER_REPLICATED") return HealthCheckResult::UNDER_REPLICATED;
  if (health == "UNAVAILABLE") return HealthCheckResult::UNAVAILABLE;
  if (health == "CONSENSUS_MISMATCH") return HealthCheckResult::CONSENSUS_MISMATCH;
  CHECK(false)  << "Unknown table health: " << health;
  __builtin_unreachable();
}
} // namespace collector
} // namespace kudu
