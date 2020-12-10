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

#include "kudu/collector/service_monitor.h"

#include <cstdint>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <prometheus/gauge.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/collector/collector_util.h"
#include "kudu/collector/prometheus_reporter.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"

DEFINE_string(collector_monitor_table_name, "system.__kudu_collector_monitor",
              "Table name of monitor table.");
DEFINE_uint32(collector_probe_monitor_table_interval_sec, 180,
              "Number of interval seconds to probe monitor table.");
DEFINE_uint32(collector_ensure_monitor_table_interval_sec, 3600,
              "Number of interval seconds to check monitor table.");
DEFINE_uint32(collector_monitor_avg_record_count_per_tablet, 100,
              "Average record count for each tablet.");
DEFINE_uint32(collector_monitor_avg_tablets_count_per_node, 3,
              "Number of tablets of monitor table on each tablet server.");
DEFINE_uint32(collector_monitor_warn_sec, 30,
              "If operations for checkintg service and record the result "
              "take more than this number of seconds, "
              "issue a warning with a trace.");
DEFINE_uint32(collector_monitor_upsert_timeout_ms, 5000,
              "Timeout for batch insert/upsert operation");
DEFINE_uint32(collector_monitor_table_replication_factor, 3,
              "Default replication factor of monitor table");

DECLARE_string(collector_cluster_name);
DECLARE_string(collector_master_addrs);
DECLARE_uint32(collector_warn_threshold_ms);
DECLARE_string(ksyncer_uuid);

using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTablet;
using kudu::client::KuduTabletServer;
using kudu::client::KuduUpsert;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;

using std::string;
using std::vector;
using std::unique_ptr;
using std::unordered_map;
using strings::Substitute;

namespace kudu {
namespace collector {

ServiceMonitor::ServiceMonitor()
  : initialized_(false),
    stop_background_threads_latch_(1) {
}

ServiceMonitor::~ServiceMonitor() {
  Shutdown();
}

Status ServiceMonitor::Init() {
  CHECK(!initialized_);

  RETURN_NOT_OK(InitMetrics());
  RETURN_NOT_OK(InitClient());
  CHECK(client_);

  last_ensure_table_time_ = MonoTime::Now();
  RETURN_NOT_OK(CheckMonitorTable());

  initialized_ = true;
  return Status::OK();
}

Status ServiceMonitor::Start() {
  CHECK(initialized_);

  RETURN_NOT_OK(StartServiceMonitorThread());

  return Status::OK();
}

void ServiceMonitor::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    stop_background_threads_latch_.CountDown();

    if (service_monitor_thread_) {
      service_monitor_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

string ServiceMonitor::ToString() {
  return "ServiceMonitor";
}

Status ServiceMonitor::InitMetrics() {
  auto& availability_family = prometheus::BuildGauge()
      .Name("service_availability_percent")
      .Help("Success percentage of operations on Kudu monitor table to "
            "indicate service availability")
      .Register(PrometheusReporter::instance()->registry());
  write_availability_percent_ = &(availability_family.Add({{"type", "write"}}));
  scan_availability_percent_ = &(availability_family.Add({{"type", "scan"}}));

  auto& latency_family = prometheus::BuildSummary()
      .Name("service_latency_ms")
      .Help("Latency of operations on Kudu monitor table, in miliseconds")
      .Register(PrometheusReporter::instance()->registry());
  write_latency_ms_ = &(latency_family.Add({{"type", "write"}}, GetQuantiles()));
  scan_latency_ms_ = &(latency_family.Add({{"type", "scan"}}, GetQuantiles()));

  return Status::OK();
}

Status ServiceMonitor::InitClient() {
  CHECK(!client_);
  const vector<string>& master_addresses =
      Split(FLAGS_collector_master_addrs, ",", strings::SkipEmpty());
  return KuduClientBuilder()
      .master_server_addrs(master_addresses)
      .Build(&client_);
}

KuduSchema ServiceMonitor::CreateTableSchema() {
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
  b.AddColumn("value")->Type(KuduColumnSchema::INT64);
  CHECK_OK(b.Build(&schema));
  return schema;
}

Status ServiceMonitor::CreateMonitorTable(const string& table_name) {
  vector<KuduTabletServer*> servers;
  ElementDeleter deleter(&servers);
  RETURN_NOT_OK(GetTabletServers(&servers));
  int32_t num_tablets =
      static_cast<int32_t>(servers.size()) * FLAGS_collector_monitor_avg_tablets_count_per_node;

  KuduSchema schema(CreateTableSchema());
  vector<string> hash_keys = {"key"};
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(table_name)
      .schema(&schema)
      .add_hash_partitions(hash_keys, num_tablets)
      .num_replicas(FLAGS_collector_monitor_table_replication_factor);
  RETURN_NOT_OK(table_creator->Create());
  LOG(INFO) << Substitute("Created table $0.", table_name);
  return Status::OK();
}

Status ServiceMonitor::CheckMonitorTable() {
  string table_name = FLAGS_collector_monitor_table_name;
  LOG(INFO) << Substitute("Checking monitor table $0.", table_name);
  bool exist = false;
  RETURN_NOT_OK(client_->TableExists(table_name, &exist));
  if (!exist) {
    RETURN_NOT_OK(CreateMonitorTable(table_name));
    RETURN_NOT_OK(client_->TableExists(table_name, &exist));
    CHECK(exist);
  }

  // Check monitor table's schema.
  KuduSchema schema;
  RETURN_NOT_OK(client_->GetTableSchema(table_name, &schema));
  if (schema != CreateTableSchema()) {
    LOG(FATAL) << Substitute("Table '$0' has an incorrect schema.", table_name);
  }

  // Check if monitor table's tablet count matches the cluster's node count.
  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(table_name, &table));
  vector<KuduScanToken*> tokens;
  ElementDeleter token_deleter(&tokens);
  KuduScanTokenBuilder builder(table.get());
  RETURN_NOT_OK(builder.Build(&tokens));
  int32_t replica_count = static_cast<int32_t>(tokens.size()) * table->num_replicas();
  vector<KuduTabletServer*> servers;
  ElementDeleter deleter(&servers);
  RETURN_NOT_OK(GetTabletServers(&servers));
  if (replica_count < servers.size()) {
    // Do not drop the table and recreate the monitor table, because the monitor table may be
    // created by user, drop it is a dangeourse operation. So just log FATAL.
    LOG(FATAL) <<
        Substitute("Table '$0' replica count doesn't match cluster's tserver count.", table_name);
  }

  // Check if all tablet servers at least has one leader replica running on it.
  unordered_map<string, vector<string>> ts_tablets;
  unordered_map<string, int> ts_leader_replica_count;
  for (const auto* token : tokens) {
    const auto& tablet =  token->tablet();
    for (const auto* replica : tablet.replicas()) {
      string ts = replica->ts().uuid();
      if (replica->is_leader()) {
        EmplaceIfNotPresent(&ts_leader_replica_count, ts, 0);
        auto& leader_count = FindOrDie(ts_leader_replica_count, ts);
        leader_count++;
      }
      EmplaceIfNotPresent(&ts_tablets, ts, vector<string>());
      auto& tablets = FindOrDie(ts_tablets, ts);
      tablets.emplace_back(tablet.id());
    }
  }
  for (const auto* server : servers) {
    const string& ts_uuid = server->uuid();
    if (ContainsKey(ts_leader_replica_count, ts_uuid)) {
      auto& leader_replica_count = FindOrDie(ts_leader_replica_count, ts_uuid);
      LOG(INFO) << Substitute("TS $0 has $1 monitor table's leader replicas on it",
                              ts_uuid, leader_replica_count);
      continue;
    }
    if (!ContainsKey(ts_tablets, ts_uuid)) {
      LOG(WARNING) << Substitute("TS $0 has no monitor table's replicas", ts_uuid);
      // Try to rebalance to relocate some replicas to this tserver.
      RETURN_NOT_OK(RebalanceMonitorTable());
      // Check again after rebalanced.
      RETURN_NOT_OK(CheckMonitorTable());
      return Status::OK();
    }

    // If this tserver has replicas but without leader replicas, try to step down some replicas
    // from other tservers.
    LOG(WARNING) << Substitute("TS $0 has no monitor table's leader replicas", ts_uuid);
    const auto& tablets = FindOrDie(ts_tablets, ts_uuid);
    string leader_step_down_tablet =
        FindLeaderStepDownTablet(ts_leader_replica_count,
                                 tablets,
                                 FLAGS_collector_monitor_avg_tablets_count_per_node);
    if (!leader_step_down_tablet.empty()) {
      RETURN_NOT_OK(CallLeaderStepDown(leader_step_down_tablet, ts_uuid));
      continue;
    }
    // Try again in case that can't ensure the source TS has
    // FLAGS_collector_monitor_avg_tablets_count_per_node count of leader replicas.
    leader_step_down_tablet = FindLeaderStepDownTablet(ts_leader_replica_count,tablets, 1);
    if (!leader_step_down_tablet.empty()) {
      RETURN_NOT_OK(CallLeaderStepDown(leader_step_down_tablet, ts_uuid));
      continue;
    }
    LOG(FATAL) << Substitute(
        "Unable to call leader_step_down for replicas on TS $0, "
        "set a larger number for 'collector_monitor_avg_tablets_count_per_node'", ts_uuid);
  }

  return Status::OK();
}

Status ServiceMonitor::RebalanceMonitorTable() {
  vector<string> args = {
    "cluster",
    "rebalance",
    FLAGS_collector_master_addrs,
    "--tables=" + FLAGS_collector_monitor_table_name
  };
  string tool_stdout;
  string tool_stderr;
  RETURN_NOT_OK_PREPEND(tools::RunKuduTool(args, &tool_stdout, &tool_stderr),
                        Substitute("out: $0, err: $1", tool_stdout, tool_stderr));
  LOG(INFO) << std::endl
            << tool_stdout;
  return Status::OK();
}

string ServiceMonitor::FindLeaderStepDownTablet(
    const unordered_map<string, int>& ts_leader_replica_count,
    const vector<string>& tablets,
    int least_num_of_leader_replicas) {
  string leader_step_down_tablet;
  for (const auto& tablet : tablets) {
    string leader_host_uuid;
    Status s = GetLeaderHost(tablet, &leader_host_uuid);
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
      continue;
    }
    CHECK(!leader_host_uuid.empty());
    auto& leader_replica_count = FindOrDie(ts_leader_replica_count, leader_host_uuid);
    if (leader_replica_count > least_num_of_leader_replicas) {
      leader_step_down_tablet = tablet;
      break;
    }
  }
  return leader_step_down_tablet;
}

Status ServiceMonitor::GetLeaderHost(const string& tablet_id, string* leader_host) {
  KuduTablet* tablet_raw = nullptr;
  RETURN_NOT_OK(client_->GetTablet(tablet_id, &tablet_raw));
  unique_ptr<KuduTablet> tablet(tablet_raw);
  for (const auto* r : tablet->replicas()) {
    if (r->is_leader()) {
      *leader_host = r->ts().uuid();
      return Status::OK();
    }
  }
  return Status::NotFound(Substitute("No leader replica found for tablet $0", tablet_id));
}

Status ServiceMonitor::GetTabletServers(vector<KuduTabletServer*>* servers) {
  DCHECK(servers);
  RETURN_NOT_OK(client_->ListTabletServers(servers));
  for (auto server = servers->begin(); server != servers->end(); ++server) {
    // TODO(yingchun): Skip to monitor KSyncer, KSyncer will be removed in short future.
    if ((*server)->uuid() == FLAGS_ksyncer_uuid) {
      delete *server;
      servers->erase(server);
      return Status::OK();
    }
  }
  return Status::OK();
}

Status ServiceMonitor::CallLeaderStepDown(const string& tablet_id,
                                          const string& new_leader_ts_uuid) {
  vector<string> args = {
    "tablet",
    "leader_step_down",
    FLAGS_collector_master_addrs,
    tablet_id,
    "--new_leader_uuid=" + new_leader_ts_uuid};
  string tool_stdout;
  string tool_stderr;
  RETURN_NOT_OK_PREPEND(tools::RunKuduTool(args, &tool_stdout, &tool_stderr),
                        Substitute("out: $0, err: $1", tool_stdout, tool_stderr));
  LOG(INFO) << std::endl
            << tool_stdout;
  return Status::OK();
}

Status ServiceMonitor::StartServiceMonitorThread() {
  return Thread::Create("collector", "service-monitor",
                        [this]() { this->ServiceMonitorThread(); },
                        &service_monitor_thread_);
}

void ServiceMonitor::ServiceMonitorThread() {
  MonoTime check_time;
  do {
    check_time = MonoTime::Now();
    CheckService();
    check_time += MonoDelta::FromSeconds(FLAGS_collector_probe_monitor_table_interval_sec);
  } while (!stop_background_threads_latch_.WaitUntil(check_time));
  LOG(INFO) << "ServiceMonitorThread exit";
}

void ServiceMonitor::CheckService() {
  int32_t elapsed_seconds = (MonoTime::Now() - last_ensure_table_time_).ToSeconds();
  if (elapsed_seconds >= FLAGS_collector_ensure_monitor_table_interval_sec) {
    WARN_NOT_OK(CheckMonitorTable(), "Unable to check monitor table");
    last_ensure_table_time_ = MonoTime::Now();
  }

  LOG(INFO) << "Start to CheckService";
  MonoTime start(MonoTime::Now());
  scoped_refptr<Trace> trace(new Trace);
  ADOPT_TRACE(trace.get());
  TRACE("init");
  bool exist = false;
  CHECK_OK(client_->TableExists(FLAGS_collector_monitor_table_name, &exist));
  if (!exist) {
    WARN_NOT_OK(CheckMonitorTable(), "Unable to check monitor table");
    CHECK_OK(client_->TableExists(FLAGS_collector_monitor_table_name, &exist));
    CHECK(exist);
  }
  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(FLAGS_collector_monitor_table_name, &table));

  WARN_NOT_OK(UpsertAndScanRows(table), "Unable to upsert and scan some rows");

  int64_t elapsed_sec = (MonoTime::Now() - start).ToSeconds();
  if (elapsed_sec > FLAGS_collector_monitor_warn_sec) {
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << "Trace:" << std::endl
                   << Trace::CurrentTrace()->DumpToString();
    }
  }
}

Status ServiceMonitor::UpsertAndScanRows(const shared_ptr<KuduTable>& table) {
  shared_ptr<KuduSession> session = table->client()->NewSession();
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  session->SetTimeoutMillis(FLAGS_collector_monitor_upsert_timeout_ms);
  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  KuduScanTokenBuilder builder(table.get());
  RETURN_NOT_OK(builder.Build(&tokens));
  int32_t record_count =
      static_cast<int32_t>(tokens.size()) * FLAGS_collector_monitor_avg_record_count_per_tablet;
  int64_t timestamp = static_cast<uint64_t>(WallTime_Now());

  // 1. Do some upsert operations.
  MonoTime start(MonoTime::Now());
  for (int32_t i = 0; i < record_count; i++) {
    KuduUpsert* upsert = table->NewUpsert();
    KuduPartialRow* row = upsert->mutable_row();
    RETURN_NOT_OK(row->SetInt64("key", i));
    RETURN_NOT_OK(row->SetInt64("value", timestamp));
    RETURN_NOT_OK(session->Apply(upsert));
  }
  // TODO(yingchun): 'write_success' is not accurate if errors overflowed.
  int32_t write_success = record_count;
  Status s = session->Flush();
  if (!s.ok()) {
    vector<KuduError*> errors;
    ElementDeleter d(&errors);
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    if (overflow) {
      LOG(WARNING) << "Error overflow occured.";
    }
    for (KuduError* error : errors) {
      LOG(WARNING) << "UPSERT FAILED: " << error->status().ToString() << ", "
                   << error->failed_op().ToString();
    }
    write_success -= errors.size();
  }
  int64_t write_latency_ms = (MonoTime::Now() - start).ToMilliseconds();
  TRACE("Upsert some rows");
  if (write_success != record_count) {
    LOG(WARNING) << Substitute("Expect to upsert $0 rows, actually upsert $1 rows.",
                                record_count, write_success);
  }

  // Check if rows upserted
  KuduScanner scanner(table.get());
  // Add a predicate: WHERE key >= 0
  KuduPredicate* p = table->NewComparisonPredicate(
      "key", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(0));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(p));
  // Add a predicate: WHERE key < record_count
  p = table->NewComparisonPredicate(
      "key", KuduPredicate::LESS, KuduValue::FromInt(record_count));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(p));
  RETURN_NOT_OK(scanner.Open());

  int read_success = 0;
  start = MonoTime::Now();
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (const auto& row : batch) {
      int64_t val;
      RETURN_NOT_OK(row->GetInt64(1, &val));
      if (PREDICT_TRUE(val == timestamp)) {
        read_success++;
      }
    }
  }
  int64_t scan_latency_ms = (MonoTime::Now() - start).ToMilliseconds();
  TRACE("Scan some rows");
  if (read_success != write_success) {
    LOG(WARNING) << Substitute("Expect to get $0 rows, actually get $1 rows.",
                                write_success, read_success);
  }

  write_availability_percent_->Set(100.0 * write_success / record_count);
  scan_availability_percent_->Set(100.0 * read_success / record_count);
  write_latency_ms_->Observe(static_cast<double>(write_latency_ms));
  scan_latency_ms_->Observe(static_cast<double>(scan_latency_ms));

  TRACE("Pushed results");

  return Status::OK();
}

} // namespace collector
} // namespace kudu
