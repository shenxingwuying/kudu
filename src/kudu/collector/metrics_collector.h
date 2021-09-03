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

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest_prod.h>
#include <prometheus/family.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/manual_summary.h>
#include <rapidjson/document.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {
class JsonReader;
class Thread;
class ThreadPool;
}  // namespace kudu

namespace kudu {

namespace collector {

class NodesChecker;

class MetricsCollector : public RefCounted<MetricsCollector> {
 public:
  explicit MetricsCollector(scoped_refptr<NodesChecker> nodes_checker);
  ~MetricsCollector();

  Status Init();
  Status Start();
  void Shutdown();

  static std::string ToString();

 private:
  struct MetricsDictionary;
  friend class RefCounted<MetricsCollector>;

  FRIEND_TEST(TestMetricsCollector, TestConvertStateToInt);
  FRIEND_TEST(TestMetricsCollector, TestGetHistValue);
  FRIEND_TEST(TestMetricsCollector, TestMergeToTableLevelMetrics);
  FRIEND_TEST(TestMetricsCollector, TestMergeToClusterLevelMetrics);
  FRIEND_TEST(TestMetricsCollector, TestParseMetrics);
  FRIEND_TEST(TestMetricsCollector, TestParseTypesOfMetrics);
  FRIEND_TEST(TestMetricsCollector, TestInitMetrics);
  FRIEND_TEST(TestMetricsCollector, TestInitFilters);
  FRIEND_TEST(TestMetricsCollector, TestInitMetricsUrlParameters);
  FRIEND_TEST(TestMetricsCollector, TestInitClusterLevelMetrics);

  // Metric name --> value, metric is in type of GAUGE or COUNTER.
  using Metrics = std::unordered_map<std::string, int64_t>;
  // Table name --> metric name-value pairs.
  using TablesMetrics = std::unordered_map<std::string, Metrics>;

  // Simple struct to collect histogram metrics.
  struct SimpleHistogram {
    // 'total_count' value in histogram metric.
    int64_t count = 0;
    int64_t sum = 0;

    // e.g. {{50, 20}     // means P50 value is 20
    //       {75, 34},
    //       {95, 59},
    //       {99, 220}}
    std::map<int, double> percentile_values;
    SimpleHistogram(int64_t c = 0,  // NOLINT(runtime/explicit)
                    int64_t s = 0,
                    std::map<int, double> pvs = {})
        : count(c), sum(s), percentile_values(std::move(pvs)) {
    }
    inline bool operator==(const SimpleHistogram& rhs) const {
      return count == rhs.count &&
             sum == rhs.sum && percentile_values == rhs.percentile_values;
    }
  };
  // A list of SimpleHistogram data reported from different instance.
  using HistMetricsRawData = std::vector<SimpleHistogram>;
  // Metric name --> HistMetricsRawData, metric is in type of HISTOGRAM.
  using HistMetrics = std::unordered_map<std::string, HistMetricsRawData>;
  // Table name --> metric name-struct pairs.
  using TablesHistMetrics = std::unordered_map<std::string, HistMetrics>;

  Status InitMetrics();
  enum NodeType {
    kMaster = 0,
    kTServer = 1,
  };
  Status InitMetricsFromNode(NodeType node_type,
                             MetricsDictionary* metrics_dict) const;
  static Status ExtractMetricTypes(const JsonReader& r,
                                   const rapidjson::Value* entity,
                                   MetricsDictionary* metrics_dict);
  void InitFilters();
  void InitMetricsUrlParameters();
  void InitHostTableLevelMetrics();
  void InitClusterLevelMetrics();

  Status StartMetricCollectorThread();
  void MetricCollectorThread();
  Status CollectAndReportMasterMetrics();
  Status CollectAndReportTServerMetrics();

  Status UpdateThreadPool(int32_t required_thread_count);

  void CollectAndReportHostLevelMetrics(NodeType node_type,
                                        const std::string& url,
                                        TablesMetrics* metrics_by_table_name,
                                        TablesHistMetrics* hist_metrics_by_table_name);

  static Status MergeToTableLevelMetrics(
      const std::vector<TablesMetrics>& hosts_metrics_by_table_name,
      const std::vector<TablesHistMetrics>& hosts_hist_metrics_by_table_name,
      TablesMetrics* metrics_by_table_name,
      TablesHistMetrics* hist_metrics_by_table_name);
  static Status MergeToClusterLevelMetrics(const TablesMetrics& metrics_by_table_name,
                                           const TablesHistMetrics& hist_metrics_by_table_name,
                                           Metrics* cluster_metrics);

  // Report metrics to third-party monitor system.
  void CollectMetrics(const std::string& endpoint,
                      const Metrics& metrics,
                      const std::string& level,
                      const std::map<std::string, std::string>& extra_tags);
  void CollectMetrics(const std::string& endpoint,
                      const HistMetrics& metrics,
                      const std::string& level,
                      const std::map<std::string, std::string>& extra_tags);

  Status ReportHostTableLevelMetrics(const std::string& host_name,
                                     const TablesMetrics& metrics_by_table_name,
                                     const TablesHistMetrics& hist_metrics_by_table_name);
  Status ReportHostLevelMetrics(const std::string& host_name,
                                NodeType node_type,
                                const Metrics& host_metrics,
                                const HistMetrics& host_hist_metrics);
  Status ReportTableLevelMetrics(const TablesMetrics& metrics_by_table_name,
                                 const TablesHistMetrics& hist_metrics_by_table_name);
  Status ReportClusterLevelMetrics(const Metrics& cluster_metrics);
  static SimpleHistogram MergeHistMetricsRawData(const HistMetricsRawData& shs);

  // Get metrics from server by http method.
  static Status GetMetrics(const std::string& url, std::string* resp);

  // Parse metrics from http response, entities may be in different types.
  Status ParseMetrics(NodeType node_type,
                      const std::string& data,
                      TablesMetrics* metrics_by_table_name,
                      Metrics* host_metrics,
                      TablesHistMetrics* hist_metrics_by_table_name,
                      HistMetrics* host_hist_metrics);
  Status ParseServerMetrics(const JsonReader& r,
                            const rapidjson::Value* entity,
                            Metrics* host_metrics,
                            HistMetrics* host_hist_metrics) const;
  Status ParseTableMetrics(const JsonReader& r,
                           const rapidjson::Value* entity,
                           TablesMetrics* metrics_by_table_name,
                           Metrics* host_metrics,
                           TablesHistMetrics* hist_metrics_by_table_name,
                           HistMetrics* host_hist_metrics) const;
  Status ParseCatalogMetrics(const JsonReader& r,
                             const rapidjson::Value* entity,
                             Metrics* tablet_metrics,
                             HistMetrics* tablet_hist_metrics) const;
  Status ParseEntityMetrics(const JsonReader& r,
                            const rapidjson::Value* entity,
                            Metrics* kv_metrics,
                            Metrics* merged_kv_metrics,
                            HistMetrics* hist_metrics,
                            HistMetrics* merged_hist_metrics) const;

  static Status GetNumberMetricValue(const rapidjson::Value* metric,
                                     const std::string& metric_name,
                                     int64_t* result);
  static Status GetStringMetricValue(const rapidjson::Value* metric,
                                     const std::string& metric_name,
                                     int64_t* result);
  static Status ConvertStateToInt(const std::string& value, int64_t* result);

  bool initialized_;

  scoped_refptr<NodesChecker> nodes_checker_;

  enum class MetricType {
    kCounter = 0,
    kGauge,
    kMeanSummary,
    kSummary
  };
  struct MetricsDictionary {
    std::unordered_map<std::string, MetricType> metric_type_;
    std::unordered_map<std::string, prometheus::Family<prometheus::Counter>*> counters_;
    std::unordered_map<std::string, prometheus::Family<prometheus::Gauge>*> gauges_;
    std::unordered_map<std::string, prometheus::Family<prometheus::ManualSummary>*> summary_;
  };
  MetricsDictionary metrics_dict_;

  // Attribute filter, attributes not in this map will be filtered if it's not empty.
  // attribute name ---> attribute values
  std::unordered_map<std::string, std::set<std::string>> attributes_filter_;
  std::string metric_url_parameters_;
  std::unordered_set<std::string> hosttable_metrics_filter_;
  Metrics cluster_metrics_filter_;

  CountDownLatch stop_background_threads_latch_;
  scoped_refptr<Thread> metric_collector_thread_;
  std::unique_ptr<ThreadPool> host_metric_collector_thread_pool_;

  DISALLOW_COPY_AND_ASSIGN(MetricsCollector);
};
} // namespace collector
} // namespace kudu
