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

#include "kudu/collector/metrics_collector.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <functional>
#include <map>
#include <ostream>
#include <set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <rapidjson/rapidjson.h>

#include "kudu/collector/collector_util.h"
#include "kudu/collector/nodes_checker.h"
#include "kudu/collector/prometheus_reporter.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"
#include "kudu/util/zlib.h"

DEFINE_bool(collector_simplify_labels, true, "Whether to simplify prometheus metric labels. "
            "If true, 'alert_level' and 'unit' labels are hided");
DEFINE_string(collector_attributes_filter, "",
              "Entity attributes to collect (semicolon-separated list of entity attribute "
              "name and values). e.g. attr_name1:attr_val1,attr_val2;attr_name2:attr_val3");
DEFINE_string(collector_cluster_level_metrics, "on_disk_size,on_disk_data_size",
              "Metric names which should be merged and pushed to cluster level view "
              "(comma-separated list of metric names)");
DEFINE_string(collector_hosttable_level_metrics, "merged_entities_count_of_tablet",
              "Host-table level metrics need to report (comma-separated list of metric names). "
              "'host-table level metrics' means the metrics merged by table name of tablets on a"
              " host.");
DEFINE_string(collector_metrics, "",
              "Metrics to collect (comma-separated list of metric names)");
DEFINE_string(collector_metrics_types_for_testing, "",
              "Only for test, used to initialize 'metric_types_'");

DECLARE_string(collector_cluster_name);
DECLARE_uint32(collector_metrics_collect_interval_sec);
DECLARE_uint32(collector_timeout_sec);
DECLARE_uint32(collector_warn_threshold_ms);

METRIC_DECLARE_gauge_size(merged_entities_count_of_tablet);
METRIC_DECLARE_gauge_uint64(live_row_count);

using rapidjson::Value;
using std::map;
using std::set;
using std::string;
using std::vector;
using std::unordered_map;
using std::unordered_set;
using strings::Substitute;

namespace {
unordered_map<string, set<string>> ParseAttributesFilter() {
  unordered_map<string, set<string>> attributes_filter;
  vector<string> attribute_values_by_name_list =
      Split(FLAGS_collector_attributes_filter, ";", strings::SkipEmpty());
  for (const auto& attribute_values_by_name : attribute_values_by_name_list) {
    vector<string> attribute_name_and_values =
        Split(attribute_values_by_name, ":", strings::SkipEmpty());
    CHECK_EQ(attribute_name_and_values.size(), 2);
    set<string> values(Split(attribute_name_and_values[1], ",", strings::SkipEmpty()));
    CHECK(!values.empty());
    EmplaceOrDie(&attributes_filter, std::make_pair(attribute_name_and_values[0], values));
  }
  return attributes_filter;
}

string GenerateMetricsRequestUrl() {
  string metric_url_parameters = "/metrics?compact=1";
  if (!FLAGS_collector_metrics.empty()) {
    metric_url_parameters += "&metrics=" + FLAGS_collector_metrics;
  }
  metric_url_parameters += "&merge_rules=tablet|table|table_name";

  auto attributes_filter = ParseAttributesFilter();
  if (!attributes_filter.empty()) {
    metric_url_parameters += "&attributes=";
  }
  for (const auto& attribute_filter : attributes_filter) {
    for (const auto& value : attribute_filter.second) {
      metric_url_parameters += Substitute("$0,$1,", attribute_filter.first, value);
    }
  }
  return metric_url_parameters;
}
}

bool ValidateMetricsCollectorFlags() {
  vector<string> cluster_collect_metrics =
      Split(FLAGS_collector_cluster_level_metrics, ",", strings::SkipEmpty());
  vector<string> all_collect_metrics_vec =
      Split(FLAGS_collector_metrics, ",", strings::SkipEmpty());
  if (all_collect_metrics_vec.empty()) {
    return true;
  }

  set<string> all_collect_metrics(all_collect_metrics_vec.begin(), all_collect_metrics_vec.end());
  for (const auto& metric : cluster_collect_metrics) {
    if (!ContainsKey(all_collect_metrics, metric)) {
      LOG(ERROR) << Substitute(
          "--collector_cluster_level_metrics($0) should be a subset of --collector_metrics($1).",
          FLAGS_collector_cluster_level_metrics, FLAGS_collector_metrics);
      return false;
    }
  }

  const int kMaxUrlLength = 2048;
  string metric_url_parameters = GenerateMetricsRequestUrl();
  if (metric_url_parameters.length() > kMaxUrlLength) {
    LOG(ERROR) << Substitute(
        "Generated url request length ($0) is too long, you should simplify --collector_metrics or "
        "--collector_attributes_filter to shorten url.", metric_url_parameters.length());
    return false;
  }

  return true;
}

GROUP_FLAG_VALIDATOR(metrics_collector_flags, ValidateMetricsCollectorFlags);

namespace kudu {
namespace collector {

MetricsCollector::MetricsCollector(scoped_refptr<NodesChecker> nodes_checker)
  : initialized_(false),
    nodes_checker_(std::move(nodes_checker)),
    stop_background_threads_latch_(1) {
}

MetricsCollector::~MetricsCollector() {
  Shutdown();
}

Status MetricsCollector::Init() {
  CHECK(!initialized_);

  RETURN_NOT_OK(InitMetrics());
  InitFilters();
  InitMetricsUrlParameters();
  InitHostTableLevelMetrics();
  InitClusterLevelMetrics();

  initialized_ = true;
  return Status::OK();
}

Status MetricsCollector::Start() {
  CHECK(initialized_);

  RETURN_NOT_OK(StartMetricCollectorThread());

  return Status::OK();
}

void MetricsCollector::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    stop_background_threads_latch_.CountDown();
    if (metric_collector_thread_) {
      metric_collector_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

string MetricsCollector::ToString() {
  return "MetricsCollector";
}

Status MetricsCollector::StartMetricCollectorThread() {
  return Thread::Create("server", "metric-collector",
                        [this]() { this->MetricCollectorThread(); },
                        &metric_collector_thread_);
}

void MetricsCollector::MetricCollectorThread() {
  MonoTime collect_time;
  do {
    collect_time = MonoTime::Now();
    WARN_NOT_OK(CollectAndReportTServerMetrics(), "Unable to collect tserver metrics");
    WARN_NOT_OK(CollectAndReportMasterMetrics(), "Unable to collect master metrics");
    collect_time += MonoDelta::FromSeconds(FLAGS_collector_metrics_collect_interval_sec);
  } while (!RunOnceMode() && !stop_background_threads_latch_.WaitUntil(collect_time));
  LOG(INFO) << "MetricCollectorThread exit";
}

Status MetricsCollector::UpdateThreadPool(int32_t required_thread_count) {
  // If thread pool has been created and has the same thread count to require.
  if (host_metric_collector_thread_pool_ &&
      host_metric_collector_thread_pool_->num_threads() == required_thread_count) {
    return Status::OK();
  }

  // Shutdown the thread pool and create a new one which has the required thread count.
  if (host_metric_collector_thread_pool_) {
    host_metric_collector_thread_pool_->Shutdown();
  }
  TRACE("Old thread pool shutdown");

  RETURN_NOT_OK(ThreadPoolBuilder("host-metric-collector")
      .set_min_threads(required_thread_count)
      .set_max_threads(required_thread_count)
      .set_idle_timeout(MonoDelta::FromMilliseconds(1))
      .Build(&host_metric_collector_thread_pool_));
  TRACE("New thread pool built");

  return Status::OK();
}

Status MetricsCollector::InitMetrics() {
  MetricsDictionary master_metrics_dict;
  RETURN_NOT_OK(InitMetricsFromNode(NodeType::kMaster, &master_metrics_dict));

  MetricsDictionary tserver_metrics_dict;
  RETURN_NOT_OK(InitMetricsFromNode(NodeType::kTServer, &tserver_metrics_dict));

  for (const auto& counter : tserver_metrics_dict.counters_) {
    const auto* counter_template = FindOrNull(master_metrics_dict.counters_, counter.first);
    if (counter_template) {
      assert((*counter_template)->GetConstantLabels() == counter.second->GetConstantLabels());
    } else {
      InsertOrDie(&master_metrics_dict.counters_, counter.first, counter.second);
      InsertOrDie(&(master_metrics_dict.metric_type_),
                  counter.first, tserver_metrics_dict.metric_type_[counter.first]);
    }
  }
  for (const auto& gauge : tserver_metrics_dict.gauges_) {
    const auto* gauge_template = FindOrNull(master_metrics_dict.gauges_, gauge.first);
    if (gauge_template) {
      assert((*gauge_template)->GetConstantLabels() == gauge.second->GetConstantLabels());
    } else {
      InsertOrDie(&master_metrics_dict.gauges_, gauge.first, gauge.second);
      InsertOrDie(&(master_metrics_dict.metric_type_),
                  gauge.first, tserver_metrics_dict.metric_type_[gauge.first]);
    }
  }
  for (const auto& histogram : tserver_metrics_dict.summary_) {
    const auto* histogram_template = FindOrNull(master_metrics_dict.summary_, histogram.first);
    if (histogram_template) {
      assert((*histogram_template)->GetConstantLabels() == histogram.second->GetConstantLabels());
    } else {
      InsertOrDie(&master_metrics_dict.summary_, histogram.first, histogram.second);
      InsertOrDie(&(master_metrics_dict.metric_type_),
                  histogram.first, tserver_metrics_dict.metric_type_[histogram.first]);
    }
  }

#define ADD_MORE_GAUGE_METRIC(metric)                                                     \
  do {                                                                                    \
    std::string name(METRIC_##metric.name());                                             \
    const auto* m = FindOrNull(master_metrics_dict.gauges_, name);                        \
    if (m) {                                                                              \
      break;                                                                              \
    }                                                                                     \
    auto& builder = prometheus::BuildGauge()                                              \
          .Name(name)                                                                     \
          .Help(METRIC_##metric.description());                                           \
      if (!FLAGS_collector_simplify_labels) {                                             \
        builder.Labels({{"unit", MetricUnit::Name(METRIC_##metric.unit())},               \
                        {"alert_level", MetricLevelName(METRIC_##metric.level())}});      \
      }                                                                                   \
      auto& gauge = builder.Register(PrometheusReporter::instance()->registry());         \
      InsertOrDie(&(master_metrics_dict.gauges_), gauge.GetName(), &gauge);               \
      InsertOrDie(&(master_metrics_dict.metric_type_), name, MetricType::kGauge);         \
 } while (false)

  // In case of initialized metrics from a table with a single tablet on a server,
  // 'merged_entities_count_of_tablet' is hidden, so we init it manully.
  ADD_MORE_GAUGE_METRIC(merged_entities_count_of_tablet);
  // 'live_row_count' is a new feature of tablet, it is hidden if the tablet/table
  // not support it, so we init it manully.
  ADD_MORE_GAUGE_METRIC(live_row_count);
#undef ADD_MORE_GAUGE_METRIC

  metrics_dict_ = master_metrics_dict;
  return Status::OK();
}

Status MetricsCollector::InitMetricsFromNode(NodeType node_type,
                                             MetricsDictionary* metrics_dict) const {
  DCHECK(metrics_dict);

  // Get merged metrics from server with schema at first to initialize metrics description.
  string resp;
  if (PREDICT_TRUE(FLAGS_collector_metrics_types_for_testing.empty())) {
    auto node_addr = node_type == NodeType::kMaster ?
        nodes_checker_->GetFirstMaster() : nodes_checker_->GetFirstTServer();
    RETURN_NOT_OK(GetMetrics(
        node_addr + "/metrics?include_schema=1&merge_rules=tablet|table|table_name", &resp));
  } else {
    resp = FLAGS_collector_metrics_types_for_testing;
  }

  // Parse and init 'metrics_dict'.
  JsonReader r(resp);
  RETURN_NOT_OK(r.Init());
  vector<const Value*> entities;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &entities));

  bool table_entity_inited = false;
  bool server_entity_inited = false;
  for (const Value* entity : entities) {
    string entity_type;
    CHECK_OK(r.ExtractString(entity, "type", &entity_type));
    if (entity_type == "table") {
      if (table_entity_inited) continue;
      RETURN_NOT_OK(ExtractMetricTypes(r, entity, metrics_dict));
      table_entity_inited = true;
    } else if (entity_type == "server") {
      if (server_entity_inited) continue;
      RETURN_NOT_OK(ExtractMetricTypes(r, entity, metrics_dict));
      server_entity_inited = true;
    } else {
      LOG(WARNING) << "Unhandled entity type: " << entity_type;
    }
  }
  return Status::OK();
}

Status MetricsCollector::ExtractMetricTypes(const JsonReader& r,
                                            const Value* entity,
                                            MetricsDictionary* metrics_dict) {
  CHECK(metrics_dict);

  vector<const Value*> metrics;
  RETURN_NOT_OK(r.ExtractObjectArray(entity, "metrics", &metrics));
  for (const Value* metric : metrics) {
    string name;
    RETURN_NOT_OK(r.ExtractString(metric, "name", &name));
    string description;
    RETURN_NOT_OK(r.ExtractString(metric, "description", &description));
    string unit;
    RETURN_NOT_OK(r.ExtractString(metric, "unit", &unit));
    string alert_level;
    if (!r.ExtractString(metric, "level", &alert_level).ok()) {
      // TODO(yingchun): histogram has no level, we shoukd fix it in server side.
      alert_level = "default";
    }

    if (HasPrefixString(name, "average_")) {
      auto& builder = prometheus::BuildManualSummary()
          .Name(name)
          .Help(description);
      if (!FLAGS_collector_simplify_labels) {
        builder.Labels({{"unit", unit}, {"alert_level", alert_level}});
      }
      auto& histogram = builder.Register(PrometheusReporter::instance()->registry());
      InsertOrDie(&(metrics_dict->summary_), histogram.GetName(), &histogram);
      InsertOrDie(&(metrics_dict->metric_type_), histogram.GetName(), MetricType::kMeanSummary);
      continue;
    }

    string type;
    RETURN_NOT_OK(r.ExtractString(metric, "type", &type));
    string upper_type;
    ToUpperCase(type, &upper_type);
    if (upper_type == "COUNTER") {
      auto& builder = prometheus::BuildCounter()
          .Name(name)
          .Help(description);
      if (!FLAGS_collector_simplify_labels) {
        builder.Labels({{"unit", unit}, {"alert_level", alert_level}});
      }
      auto& counter = builder.Register(PrometheusReporter::instance()->registry());
      InsertOrDie(&(metrics_dict->counters_), counter.GetName(), &counter);
      InsertOrDie(&(metrics_dict->metric_type_), counter.GetName(), MetricType::kCounter);
      continue;
    }

    if (upper_type == "GAUGE") {
      auto& builder = prometheus::BuildGauge()
          .Name(name)
          .Help(description);
      if (!FLAGS_collector_simplify_labels) {
        builder.Labels({{"unit", unit}, {"alert_level", alert_level}});
      }
      auto& gauge = builder.Register(PrometheusReporter::instance()->registry());
      InsertOrDie(&(metrics_dict->gauges_), gauge.GetName(), &gauge);
      InsertOrDie(&(metrics_dict->metric_type_), gauge.GetName(), MetricType::kGauge);
      continue;
    }

    if (upper_type == "HISTOGRAM") {
      auto& builder = prometheus::BuildManualSummary()
          .Name(name)
          .Help(description);
      if (!FLAGS_collector_simplify_labels) {
        builder.Labels({{"unit", unit}, {"alert_level", alert_level}});
      }
      auto& histogram = builder.Register(PrometheusReporter::instance()->registry());
      InsertOrDie(&(metrics_dict->summary_), histogram.GetName(), &histogram);
      InsertOrDie(&(metrics_dict->metric_type_), histogram.GetName(), MetricType::kSummary);
      continue;
    }

    CHECK(false) << Substitute("Unknown metric type: $0, name: $1", type, name);
    __builtin_unreachable();
  }
  return Status::OK();
}

void MetricsCollector::InitFilters() {
  attributes_filter_ = ParseAttributesFilter();
}

void MetricsCollector::InitMetricsUrlParameters() {
  metric_url_parameters_ = GenerateMetricsRequestUrl();
}

void MetricsCollector::InitHostTableLevelMetrics() {
  unordered_set<string> hosttable_metrics(
      Split(FLAGS_collector_hosttable_level_metrics, ",", strings::SkipEmpty()));
  hosttable_metrics_filter_.swap(hosttable_metrics);
}

void MetricsCollector::InitClusterLevelMetrics() {
  Metrics cluster_metrics;
  vector<string> metric_names =
      Split(FLAGS_collector_cluster_level_metrics, ",", strings::SkipEmpty());
  for (const auto& metric_name : metric_names) {
    cluster_metrics[metric_name] = 0;
  }
  cluster_metrics_filter_.swap(cluster_metrics);
}

Status MetricsCollector::CollectAndReportMasterMetrics() {
  LOG(INFO) << "Start to CollectAndReportMasterMetrics";
  MonoTime start(MonoTime::Now());
  scoped_refptr<Trace> trace(new Trace);
  ADOPT_TRACE(trace.get());
  TRACE("init");
  vector<string> master_http_addrs = nodes_checker_->GetMasters();
  TRACE("Nodes got");
  if (master_http_addrs.empty()) {
    return Status::OK();
  }
  RETURN_NOT_OK(UpdateThreadPool(std::max(host_metric_collector_thread_pool_->num_threads(),
                                          static_cast<int32_t>(master_http_addrs.size()))));
  for (int i = 0; i < master_http_addrs.size(); ++i) {
    RETURN_NOT_OK(host_metric_collector_thread_pool_->Submit(
      std::bind(&MetricsCollector::CollectAndReportHostLevelMetrics,
                this,
                NodeType::kMaster,
                master_http_addrs[i] + metric_url_parameters_,
                nullptr,
                nullptr)));
  }
  TRACE("Thead pool jobs submitted");
  host_metric_collector_thread_pool_->Wait();
  TRACE("Thead pool jobs done");

  int64_t elapsed_ms = (MonoTime::Now() - start).ToMilliseconds();
  if (elapsed_ms > FLAGS_collector_warn_threshold_ms) {
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << "Trace:" << std::endl
                   << Trace::CurrentTrace()->DumpToString();
    }
  }

  return Status::OK();
}

Status MetricsCollector::CollectAndReportTServerMetrics() {
  LOG(INFO) << "Start to CollectAndReportTServerMetrics";
  MonoTime start(MonoTime::Now());
  scoped_refptr<Trace> trace(new Trace);
  ADOPT_TRACE(trace.get());
  TRACE("init");
  vector<string> tserver_http_addrs = nodes_checker_->GetTServers();
  TRACE("Nodes got");
  if (tserver_http_addrs.empty()) {
    return Status::OK();
  }
  RETURN_NOT_OK(UpdateThreadPool(static_cast<int32_t>(tserver_http_addrs.size())));
  vector<TablesMetrics> hosts_metrics_by_table_name(tserver_http_addrs.size());
  vector<TablesHistMetrics> hosts_hist_metrics_by_table_name(tserver_http_addrs.size());
  for (int i = 0; i < tserver_http_addrs.size(); ++i) {
    RETURN_NOT_OK(host_metric_collector_thread_pool_->Submit(
      std::bind(&MetricsCollector::CollectAndReportHostLevelMetrics,
                this,
                NodeType::kTServer,
                tserver_http_addrs[i] + metric_url_parameters_,
                &hosts_metrics_by_table_name[i],
                &hosts_hist_metrics_by_table_name[i])));
  }
  TRACE("Thead pool jobs submitted");
  host_metric_collector_thread_pool_->Wait();
  TRACE("Thead pool jobs done");

  // Merge to table level metrics.
  TablesMetrics metrics_by_table_name;
  TablesHistMetrics hist_metrics_by_table_name;
  RETURN_NOT_OK(MergeToTableLevelMetrics(hosts_metrics_by_table_name,
                                         hosts_hist_metrics_by_table_name,
                                         &metrics_by_table_name,
                                         &hist_metrics_by_table_name));

  // Merge to cluster level metrics.
  Metrics cluster_metrics(cluster_metrics_filter_);
  RETURN_NOT_OK(MergeToClusterLevelMetrics(metrics_by_table_name,
                                           hist_metrics_by_table_name,
                                           &cluster_metrics));

  // Push table level metrics.
  RETURN_NOT_OK(ReportTableLevelMetrics(metrics_by_table_name,
                                        hist_metrics_by_table_name));

  // Push cluster level metrics.
  RETURN_NOT_OK(ReportClusterLevelMetrics(cluster_metrics));

  int64_t elapsed_ms = (MonoTime::Now() - start).ToMilliseconds();
  if (elapsed_ms > FLAGS_collector_warn_threshold_ms) {
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << "Trace:" << std::endl
                   << Trace::CurrentTrace()->DumpToString();
    }
  }

  return Status::OK();
}

Status MetricsCollector::MergeToTableLevelMetrics(
  const vector<TablesMetrics>& hosts_metrics_by_table_name,
  const vector<TablesHistMetrics>& hosts_hist_metrics_by_table_name,
  TablesMetrics* metrics_by_table_name,
  TablesHistMetrics* hist_metrics_by_table_name) {
  CHECK(metrics_by_table_name);
  CHECK(hist_metrics_by_table_name);

  // GAUGE/COUNTER type metrics.
  int metrics_count = 0;
  for (const auto& host_metrics_by_table_name : hosts_metrics_by_table_name) {
    for (const auto& table_metrics1 : host_metrics_by_table_name) {
      const auto& table_name = table_metrics1.first;
      const auto& metrics = table_metrics1.second;
      metrics_count += metrics.size();
      if (EmplaceIfNotPresent(metrics_by_table_name, std::make_pair(table_name, metrics))) {
        continue;
      }
      // This table has been fetched by some other tserver.
      auto& table_metrics = FindOrDie(*metrics_by_table_name, table_name);
      for (const auto& metric_value : metrics) {
        const auto& metric = metric_value.first;
        const auto& value = metric_value.second;
        if (EmplaceIfNotPresent(&table_metrics, std::make_pair(metric, value))) {
          continue;
        }
        // This metric has been fetched by some other tserver.
        auto& old_value = FindOrDie(table_metrics, metric);
        old_value += value;
      }
    }
  }
  TRACE(Substitute("Table GAUGE/COUNTER type metrics merged, count $0", metrics_count));

  // HISTOGRAM type metrics.
  metrics_count = 0;
  for (const auto& host_hist_metrics_by_table_name : hosts_hist_metrics_by_table_name) {
    for (const auto& table_hist_metrics1 : host_hist_metrics_by_table_name) {
      const auto& table_name = table_hist_metrics1.first;
      const auto& metrics = table_hist_metrics1.second;
      metrics_count += metrics.size();
      if (EmplaceIfNotPresent(hist_metrics_by_table_name, std::make_pair(table_name, metrics))) {
        continue;
      }
      // This table has been fetched by some other tserver.
      auto& table_hist_metrics = FindOrDie(*hist_metrics_by_table_name, table_name);
      for (const auto& metric_hist_values : metrics) {
        const auto& metric = metric_hist_values.first;
        const auto& hist_values = metric_hist_values.second;
        if (EmplaceIfNotPresent(&table_hist_metrics, std::make_pair(metric, hist_values))) {
          continue;
        }
        // This metric has been fetched by some other tserver.
        auto& old_hist_rawdata = FindOrDie(table_hist_metrics, metric);
        for (const auto& hist_value : hist_values) {
          old_hist_rawdata.emplace_back(hist_value);
        }
      }
    }
  }
  TRACE(Substitute("Table HISTOGRAM type metrics merged, count $0", metrics_count));

  return Status::OK();
}

Status MetricsCollector::MergeToClusterLevelMetrics(
    const TablesMetrics& metrics_by_table_name,
    const TablesHistMetrics& /*hist_metrics_by_table_name*/,
    Metrics* cluster_metrics) {
  CHECK(cluster_metrics);
  if (!cluster_metrics->empty()) {
    for (const auto& table_metrics : metrics_by_table_name) {
      for (auto& cluster_metric : *cluster_metrics) {
        auto *find = FindOrNull(table_metrics.second, cluster_metric.first);
        if (find) {
          cluster_metric.second += *find;
        }
      }
    }
  }
  TRACE(Substitute("Cluster metrics merged, count $0", cluster_metrics->size()));

  return Status::OK();
}

Status MetricsCollector::GetNumberMetricValue(const rapidjson::Value* metric,
                                              const string& metric_name /*metric_name*/,
                                              int64_t* result) {
  CHECK(result);
  if (metric->IsUint64() || metric->IsInt64() || metric->IsUint() || metric->IsInt()) {
    *result = metric->GetInt64();
    return Status::OK();
  }

  if (metric->IsDouble()) {
    double result_temp = metric->GetDouble();
    // Multiply by 1000000 and convert to int64_t to avoid much data loss and keep compatibility
    // with monitor system like Falcon.
    *result = static_cast<int64_t>(result_temp * 1000000);
    return Status::OK();
  }

  return Status::NotSupported(Substitute("unsupported metric $0", metric_name));
}

Status MetricsCollector::GetStringMetricValue(const Value* metric,
                                              const string& metric_name,
                                              int64_t* result) {
  CHECK(result);
  string value(metric->GetString());
  if (metric_name == "state") {
    return ConvertStateToInt(value, result);
  }
  return Status::NotSupported(Substitute("unsupported metric $0", metric_name));
}

Status MetricsCollector::ConvertStateToInt(const string& value, int64_t* result) {
  CHECK(result);
  // TODO(yingchun): Here, table state is merged by several original tablet states, which is
  // contacted by several sub-strings, like 'RUNNING', 'BOOTSTRAPPING', etc. It's tricky to
  // fetch state now, we will improve in server side later.
  const char* running = "RUNNING";
  if (value.empty() || value.size() % strlen(running) != 0) {
    *result = 0;
    return Status::OK();
  }
  for (int i = 0; i < value.size(); i += strlen(running)) {
    if (0 != strncmp(running, value.c_str() + i, strlen(running))) {
      *result = 0;
      return Status::OK();
    }
  }
  *result = 1;
  return Status::OK();
}

Status MetricsCollector::ParseServerMetrics(const JsonReader& r,
                                            const rapidjson::Value* entity,
                                            Metrics* host_metrics,
                                            HistMetrics* host_hist_metrics) const {
  CHECK(entity);
  CHECK(host_metrics);
  CHECK(host_hist_metrics);

  string server_type;
  RETURN_NOT_OK(r.ExtractString(entity, "id", &server_type));
  CHECK(server_type == "kudu.tabletserver" || server_type == "kudu.master") << server_type;

  RETURN_NOT_OK(ParseEntityMetrics(r, entity, host_metrics, nullptr, host_hist_metrics, nullptr));

  return Status::OK();
}

Status MetricsCollector::ParseTableMetrics(const JsonReader& r,
                                           const rapidjson::Value* entity,
                                           TablesMetrics* metrics_by_table_name,
                                           Metrics* host_metrics,
                                           TablesHistMetrics* hist_metrics_by_table_name,
                                           HistMetrics* host_hist_metrics) const {
  CHECK(entity);
  CHECK(metrics_by_table_name);
  CHECK(host_metrics);
  CHECK(hist_metrics_by_table_name);
  CHECK(host_hist_metrics);

  string table_name;
  RETURN_NOT_OK(r.ExtractString(entity, "id", &table_name));
  CHECK(!ContainsKey(*metrics_by_table_name, table_name));
  CHECK(!ContainsKey(*hist_metrics_by_table_name, table_name));

  EmplaceOrDie(metrics_by_table_name, std::make_pair(table_name, Metrics()));
  auto& table_metrics = FindOrDie(*metrics_by_table_name, table_name);

  EmplaceOrDie(hist_metrics_by_table_name, std::make_pair(table_name, HistMetrics()));
  auto& table_hist_metrics = FindOrDie(*hist_metrics_by_table_name, table_name);

  RETURN_NOT_OK(ParseEntityMetrics(r, entity,
      &table_metrics, host_metrics, &table_hist_metrics, host_hist_metrics));

  return Status::OK();
}

Status MetricsCollector::ParseCatalogMetrics(const JsonReader& r,
                                             const rapidjson::Value* entity,
                                             Metrics* tablet_metrics,
                                             HistMetrics* tablet_hist_metrics) const {
  CHECK(entity);
  CHECK(tablet_metrics);
  CHECK(tablet_hist_metrics);

  string tablet_id;
  RETURN_NOT_OK(r.ExtractString(entity, "id", &tablet_id));
  if (tablet_id != "sys.catalog") {  // Only used to parse 'sys.catalog'.
    return Status::OK();
  }

  RETURN_NOT_OK(ParseEntityMetrics(r, entity,
                                   tablet_metrics, nullptr,
                                   tablet_hist_metrics, nullptr));

  return Status::OK();
}

Status MetricsCollector::ParseEntityMetrics(const JsonReader& r,
                                            const rapidjson::Value* entity,
                                            Metrics* kv_metrics,
                                            Metrics* merged_kv_metrics,
                                            HistMetrics* hist_metrics,
                                            HistMetrics* merged_hist_metrics) const {
  CHECK(entity);
  CHECK(kv_metrics);
  CHECK(hist_metrics);

  vector<const Value*> metrics;
  RETURN_NOT_OK(r.ExtractObjectArray(entity, "metrics", &metrics));
  for (const Value* metric : metrics) {
    string name;
    RETURN_NOT_OK(r.ExtractString(metric, "name", &name));
    const auto* known_type = FindOrNull(metrics_dict_.metric_type_, name);
    if (!known_type) {
      LOG(ERROR) << Substitute("metric $0 has unknown type, ignore it", name);
      continue;
    }

    switch (*known_type) {
      case MetricType::kCounter:
      case MetricType::kGauge: {
        int64_t value = 0;
        const Value* val;
        RETURN_NOT_OK(r.ExtractField(metric, "value", &val));
        rapidjson::Type type = val->GetType();
        switch (type) {
          case rapidjson::Type::kStringType:
            RETURN_NOT_OK(GetStringMetricValue(val, name, &value));
            break;
          case rapidjson::Type::kNumberType:
            RETURN_NOT_OK(GetNumberMetricValue(val, name, &value));
            break;
          case rapidjson::Type::kFalseType:
          case rapidjson::Type::kTrueType:
            // Do not process true/false type.
            break;
          default:
            LOG(WARNING) << "Unknown type, metrics name: " << name;
        }

        EmplaceOrDie(kv_metrics, std::make_pair(name, value));
        if (merged_kv_metrics &&
            !EmplaceIfNotPresent(merged_kv_metrics, std::make_pair(name, value))) {
          auto& found_metric = FindOrDie(*merged_kv_metrics, name);
          found_metric += value;
        }
        break;
      }
      case MetricType::kMeanSummary: {
        double total_count;
        RETURN_NOT_OK(r.ExtractDouble(metric, "total_count", &total_count));
        double total_sum;
        RETURN_NOT_OK(r.ExtractDouble(metric, "total_sum", &total_sum));
        double value;
        RETURN_NOT_OK(r.ExtractDouble(metric, "value", &value));
        HistMetricsRawData mean_rawdata({{static_cast<int64_t>(total_count),
                                          static_cast<int64_t>(total_sum),
                                          {{50, value}}}});
        EmplaceOrDie(hist_metrics, std::make_pair(name, mean_rawdata));
        if (merged_hist_metrics &&
            !EmplaceIfNotPresent(merged_hist_metrics, std::make_pair(name, mean_rawdata))) {
          auto& found_hist_metric = FindOrDie(*merged_hist_metrics, name);
          found_hist_metric.emplace_back(mean_rawdata[0]);
        }
        break;
      }
      case MetricType::kSummary: {
        int64_t total_count;
        RETURN_NOT_OK(r.ExtractInt64(metric, "total_count", &total_count));
        int64_t total_sum;
        RETURN_NOT_OK(r.ExtractInt64(metric, "total_sum", &total_sum));

        SimpleHistogram sh;
        sh.count = total_count;
        sh.sum = total_sum;
        for (const auto& percentile : GetPercentiles()) {
          double percentile_value;
          RETURN_NOT_OK(r.ExtractDouble(metric, percentile.first.c_str(), &percentile_value));
          sh.percentile_values.emplace(percentile.second, percentile_value);
        }

        HistMetricsRawData percentile_rawdata({sh});
        if (!EmplaceIfNotPresent(hist_metrics, std::make_pair(name, percentile_rawdata))) {
          auto& found_percentile_rawdata = FindOrDie(*hist_metrics, name);
          found_percentile_rawdata.push_back(sh);
        }
        if (merged_hist_metrics &&
            !EmplaceIfNotPresent(merged_hist_metrics, std::make_pair(name, percentile_rawdata))) {
          auto& found_percentile_rawdata = FindOrDie(*merged_hist_metrics, name);
          found_percentile_rawdata.push_back(sh);
        }
        break;
      }
      default:
        CHECK(false) << "Unknown metric type, metric name: " << name;
        __builtin_unreachable();
    }
  }

  return Status::OK();
}

void MetricsCollector::CollectAndReportHostLevelMetrics(
    NodeType node_type,
    const string& url,
    TablesMetrics* metrics_by_table_name,
    TablesHistMetrics* hist_metrics_by_table_name) {
  MonoTime start(MonoTime::Now());
  scoped_refptr<Trace> trace(new Trace);
  ADOPT_TRACE(trace.get());
  TRACE("init");

  string host_name = ExtractHostName(url);

  // Get metrics from server.
  string resp;
  KUDU_WARN_NOT_OK_AND_RETURN(GetMetrics(url, &resp),
                              Substitute("GetMetrics from $0 error", host_name));

  // Merge metrics by table and metric type.
  Metrics host_metrics;
  HistMetrics host_hist_metrics;
  KUDU_WARN_NOT_OK_AND_RETURN(ParseMetrics(node_type, resp, metrics_by_table_name, &host_metrics,
                                           hist_metrics_by_table_name, &host_hist_metrics),
                              Substitute("ParseMetrics for $0 error", host_name));

  // Host table level.
  if (metrics_by_table_name && hist_metrics_by_table_name) {
    KUDU_WARN_NOT_OK_AND_RETURN(ReportHostTableLevelMetrics(host_name,
                                                            *metrics_by_table_name,
                                                            *hist_metrics_by_table_name),
                                Substitute("ReportHostTableLevelMetrics for $0 error", host_name));
  }

  // Host level.
  KUDU_WARN_NOT_OK_AND_RETURN(ReportHostLevelMetrics(host_name,
                                                     node_type,
                                                     host_metrics,
                                                     host_hist_metrics),
                              Substitute("ReportHostLevelMetrics for $0 error", host_name));

  int64_t elapsed_ms = (MonoTime::Now() - start).ToMilliseconds();
  if (elapsed_ms > FLAGS_collector_warn_threshold_ms) {
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << "Trace:" << std::endl
                   << Trace::CurrentTrace()->DumpToString();
    }
  }
}

Status MetricsCollector::ParseMetrics(NodeType node_type,
                                      const string& data,
                                      TablesMetrics* metrics_by_table_name,
                                      Metrics* host_metrics,
                                      TablesHistMetrics* hist_metrics_by_table_name,
                                      HistMetrics* host_hist_metrics) {
  JsonReader r(data);
  RETURN_NOT_OK(r.Init());
  vector<const Value*> entities;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &entities));

  for (const Value* entity : entities) {
    string entity_type;
    RETURN_NOT_OK(r.ExtractString(entity, "type", &entity_type));
    if (entity_type == "server") {
      RETURN_NOT_OK(ParseServerMetrics(r, entity, host_metrics, host_hist_metrics));
    } else if (entity_type == "table") {
      if (NodeType::kMaster == node_type) {
        RETURN_NOT_OK(ParseCatalogMetrics(r, entity, host_metrics, host_hist_metrics));
      } else {
        CHECK(NodeType::kTServer == node_type);
        RETURN_NOT_OK(ParseTableMetrics(r,
                                        entity,
                                        metrics_by_table_name,
                                        host_metrics,
                                        hist_metrics_by_table_name,
                                        host_hist_metrics));
      }
    } else {
      LOG(FATAL) << "Unknown entity_type: " << entity_type;
    }
  }
  TRACE(Substitute("Metrics parsed, entity count $0", entities.size()));

  return Status::OK();
}

void MetricsCollector::CollectMetrics(const string& endpoint,
                                      const Metrics& metrics,
                                      const string& level,
                                      const map<string, string>& extra_tags) {
  for (const auto& metric : metrics) {
    auto counter = FindOrNull(metrics_dict_.counters_, metric.first);
    if (counter) {
      map<string, string> tags(extra_tags);
      EmplaceOrDie(&tags, std::make_pair("endpoint", endpoint));
      EmplaceOrDie(&tags, std::make_pair("level", level));
      auto& m = (*counter)->Add(tags);
      m.Increment(metric.second - m.Value());
    } else {
      auto* gauge = FindOrNull(metrics_dict_.gauges_, metric.first);
      CHECK(gauge);
      map<string, string> tags(extra_tags);
      EmplaceOrDie(&tags, std::make_pair("endpoint", endpoint));
      EmplaceOrDie(&tags, std::make_pair("level", level));
      auto& m = (*gauge)->Add(tags);
      m.Set(metric.second);
    }
  }
}

void MetricsCollector::CollectMetrics(const string& endpoint,
                                      const HistMetrics& metrics,
                                      const string& level,
                                      const map<string, string>& extra_tags) {
  for (const auto& metric : metrics) {
    auto* histogram = FindOrNull(metrics_dict_.summary_, metric.first);
    CHECK(histogram);
    map<string, string> tags(extra_tags);
    EmplaceOrDie(&tags, std::make_pair("endpoint", endpoint));
    EmplaceOrDie(&tags, std::make_pair("level", level));
    auto& m = (*histogram)->Add(tags);
    SimpleHistogram sh = MergeHistMetricsRawData(metric.second);
    m.SetCountAndSum(sh.count, sh.sum);
    for (const auto& percentile_value : sh.percentile_values) {
      m.AddQuantile(percentile_value.first / 100.0, percentile_value.second);
    }
  }
}

Status MetricsCollector::ReportHostTableLevelMetrics(
    const string& host_name,
    const TablesMetrics& metrics_by_table_name,
    const TablesHistMetrics& hist_metrics_by_table_name) {
  // GAUGE/COUNTER type metrics.
  int metrics_count = 0;
  for (const auto& table_metrics : metrics_by_table_name) {
    const map<string, string> extra_tag({{"table", table_metrics.first}});
    Metrics filtered_metrics;
    for (const auto& metric : table_metrics.second) {
      if (ContainsKey(hosttable_metrics_filter_, metric.first)) {
        filtered_metrics.insert(metric);
      }
    }
    metrics_count += filtered_metrics.size();
    CollectMetrics(host_name, filtered_metrics, "host_table", extra_tag);
  }
  TRACE(Substitute("Host-table GAUGE/COUNTER type metrics collected, count $0", metrics_count));

  // HISTOGRAM type metrics.
  int hist_metrics_count = 0;
  for (const auto& table_hist_metrics : hist_metrics_by_table_name) {
    const map<string, string> extra_tag({{"table", table_hist_metrics.first}});
    HistMetrics filtered_metrics;
    for (const auto& metric : table_hist_metrics.second) {
      if (ContainsKey(hosttable_metrics_filter_, metric.first)) {
        filtered_metrics.insert(metric);
      }
    }
    hist_metrics_count += table_hist_metrics.second.size();
    CollectMetrics(host_name, filtered_metrics, "host_table", extra_tag);
  }
  TRACE(Substitute("Host-table HISTOGRAM type metrics collected, count $0", hist_metrics_count));

  TRACE(Substitute("Host-table metrics reported, count $0", metrics_count + hist_metrics_count));

  return Status::OK();
}

Status MetricsCollector::ReportHostLevelMetrics(
    const string& host_name,
    NodeType node_type,
    const Metrics& host_metrics,
    const HistMetrics& host_hist_metrics) {
  static const vector<map<string, string>> role_tag({{{"role", "master"}}, {{"role", "tserver"}}});
  // GAUGE/COUNTER type metrics.
  CollectMetrics(host_name, host_metrics, "host", role_tag[node_type]);
  TRACE(Substitute("Host GAUGE/COUNTER type metrics collected, count $0", host_metrics.size()));

  // HISTOGRAM type metrics.
  CollectMetrics(host_name, host_hist_metrics, "host", role_tag[node_type]);
  TRACE(Substitute("Host HISTOGRAM type metrics collected, count $0", host_hist_metrics.size()));

  TRACE(Substitute("Host metrics reported, count $0",
                   host_metrics.size() + host_hist_metrics.size()));

  return Status::OK();
}

Status MetricsCollector::ReportTableLevelMetrics(
    const TablesMetrics& metrics_by_table_name,
    const TablesHistMetrics& hist_metrics_by_table_name) {
  // GAUGE/COUNTER type metrics.
  int metrics_count = 0;
  for (const auto& table_metrics : metrics_by_table_name) {
    metrics_count += table_metrics.second.size();
    CollectMetrics(table_metrics.first, table_metrics.second, "table", {});
  }
  TRACE(Substitute("Table GAUGE/COUNTER type metrics collected, count $0", metrics_count));

  // HISTOGRAM type metrics.
  int hist_metrics_count = 0;
  for (const auto& table_hist_metrics : hist_metrics_by_table_name) {
    hist_metrics_count += table_hist_metrics.second.size();
    CollectMetrics(table_hist_metrics.first, table_hist_metrics.second, "table", {});
  }
  TRACE(Substitute("Table HISTOGRAM type metrics collected, count $0", hist_metrics_count));

  TRACE(Substitute("Table metrics reported, count $0", metrics_count + hist_metrics_count));

  return Status::OK();
}

Status MetricsCollector::ReportClusterLevelMetrics(const Metrics& cluster_metrics) {
  CollectMetrics(FLAGS_collector_cluster_name, cluster_metrics, "cluster", {});
  TRACE(Substitute("Cluster metrics collected, count $0", cluster_metrics.size()));

  TRACE(Substitute("Cluster metrics reported, count $0", cluster_metrics.size()));
  return Status::OK();
}

MetricsCollector::SimpleHistogram MetricsCollector::MergeHistMetricsRawData(
    const HistMetricsRawData& shs) {
  SimpleHistogram result;
  for (const auto& sh : shs) {
    result.count += sh.count;
    result.sum += sh.sum;
    for (const auto& percentile_value : sh.percentile_values) {
      double sum = percentile_value.second * sh.count;
      if (!EmplaceIfNotPresent(&(result.percentile_values),
                               std::make_pair(percentile_value.first, sum))) {
        auto& found_percentile_values = FindOrDie(result.percentile_values, percentile_value.first);
        found_percentile_values += sum;
      }
    }
  }
  for (auto& percentile_value : result.percentile_values) {
    if (result.count != 0) {
      percentile_value.second = std::llround(percentile_value.second / result.count);
    }
  }

  return result;
}

Status MetricsCollector::GetMetrics(const string& url, string* resp) {
  CHECK(resp);
  EasyCurl curl;
  faststring dst;
  RETURN_NOT_OK(curl.FetchURL(url, &dst, {"Accept-Encoding: gzip"}));
  std::ostringstream oss;
  string dst_str = dst.ToString();
  if (zlib::Uncompress(Slice(dst_str), &oss).ok()) {
    *resp = oss.str();
  } else {
    *resp = dst_str;
  }
  TRACE(Substitute("Metrics got from server: $0", url));

  return Status::OK();
}
} // namespace collector
} // namespace kudu
