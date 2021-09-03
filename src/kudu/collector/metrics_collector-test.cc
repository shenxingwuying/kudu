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

#include <cstdint>

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/collector/nodes_checker.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/test_macros.h"

DECLARE_string(collector_attributes_filter);
DECLARE_string(collector_cluster_level_metrics);
DECLARE_string(collector_metrics);
DECLARE_string(collector_metrics_types_for_testing);

using std::set;
using std::string;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace collector {

scoped_refptr<MetricsCollector> BuildCollector() {
  scoped_refptr<NodesChecker> nodes_checker(new NodesChecker());
  return new MetricsCollector(nodes_checker);
}

TEST(TestMetricsCollector, TestConvertStateToInt) {
  int64_t result = 1;
  ASSERT_OK(MetricsCollector::ConvertStateToInt("", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("STOPPED", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNINGSTOPPED", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNINGBOOTSTRAPPING", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNING", &result));
  ASSERT_EQ(result, 1);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNINGRUNNING", &result));
  ASSERT_EQ(result, 1);
}

TEST(TestMetricsCollector, TestGetHistValue) {
  MetricsCollector::SimpleHistogram sh1({1, 10, {{75, 10},
                                                 {95, 10},
                                                 {99, 10}}});
  MetricsCollector::SimpleHistogram sh2({2, 30, {{75, 20},
                                                 {95, 20},
                                                 {99, 20}}});
  MetricsCollector::SimpleHistogram sh1_1({2, 20, {{75, 10},
                                                   {95, 10},
                                                   {99, 10}}});
  MetricsCollector::SimpleHistogram sh1_2({3, 40, {{75, 17},
                                                   {95, 17},
                                                   {99, 17}}});
  ASSERT_EQ(sh1, MetricsCollector::MergeHistMetricsRawData({sh1}));
  ASSERT_EQ(sh1_1, MetricsCollector::MergeHistMetricsRawData({sh1, sh1}));
  ASSERT_EQ(sh1_2, MetricsCollector::MergeHistMetricsRawData({sh1, sh2}));
}

TEST(TestMetricsCollector, TestMergeToTableLevelMetrics) {
  // Merge empty metrics.
  {
    vector<MetricsCollector::TablesMetrics> hosts_tables_metrics;
    vector<MetricsCollector::TablesHistMetrics> hosts_tables_hist_metrics;
    MetricsCollector::TablesMetrics tables_metrics;
    MetricsCollector::TablesHistMetrics tables_hist_metrics;
    ASSERT_OK(MetricsCollector::MergeToTableLevelMetrics(
      hosts_tables_metrics, hosts_tables_hist_metrics,
      &tables_metrics, &tables_hist_metrics));
    ASSERT_TRUE(tables_metrics.empty());
    ASSERT_TRUE(tables_hist_metrics.empty());
  }
  // Merge multi metrics.
  {
    vector<MetricsCollector::TablesMetrics> hosts_tables_metrics({
        {  // host-1
          {"table1",
           {{"metric1", 1},
            {"metric2", 2}}},
          {"table2",
           {{"metric1", 100},
            {"metric3", 200}}}},
        {  // host-2
          {"table1",
           {{"metric1", 100},
            {"metric2", 200}}},
          {"table2",
           {{"metric1", 1},
            {"metric2", 2}}},
          {"table3",
           {{"metric1", 1},
            {"metric2", 2}}}}
    });
    vector<MetricsCollector::TablesHistMetrics> hosts_tables_hist_metrics({
        {  // host-1
          {"table1",
           {{"metric3",
             {{10, 10, {{99, 100}}},
              {20, 20, {{99, 200}}}}},
            {"metric4",
             {{30, 30, {{99, 300}}},
              {40, 40, {{99, 400}}}}}}},
          {"table2",
           {{"metric3",
             {{10, 10, {{99, 200}}},
              {20, 20, {{99, 300}}}}},
            {"metric4",
             {{40, 40, {{99, 300}}},
              {50, 50, {{99, 400}}}}}}}},
        {  // host-2
          {"table1",
           {{"metric3",
             {{10, 10, {{99, 100}}},
              {20, 20, {{99, 200}}}}},
            {"metric4",
             {{30, 30, {{99, 300}}},
              {40, 40, {{99, 400}}}}}}},
          {"table2",
           {{"metric3",
             {{10, 10, {{99, 200}}},
              {20, 20, {{99, 300}}}}},
            {"metric4",
             {{40, 40, {{99, 300}}},
              {50, 50, {{99, 400}}}}}}},
          {"table3",
           {{"metric3",
             {{10, 10, {{99, 200}}},
              {20, 20, {{99, 300}}}}},
            {"metric4",
             {{40, 40, {{99, 300}}},
              {50, 50, {{99, 400}}}}}}}}
    });
    MetricsCollector::TablesMetrics tables_metrics;
    MetricsCollector::TablesHistMetrics tables_hist_metrics;
    ASSERT_OK(MetricsCollector::MergeToTableLevelMetrics(
        hosts_tables_metrics, hosts_tables_hist_metrics,
        &tables_metrics, &tables_hist_metrics));
    ASSERT_EQ(tables_metrics, MetricsCollector::TablesMetrics({
        {"table1",
         {{"metric1", 101},
          {"metric2", 202}}},
        {"table2",
         {{"metric1", 101},
          {"metric2", 2},
          {"metric3", 200}}},
        {"table3",
         {{"metric1", 1},
          {"metric2", 2}}}
    }));
    ASSERT_EQ(tables_hist_metrics, MetricsCollector::TablesHistMetrics({
        {"table1",
         {{"metric3",
           {{10, 10, {{99, 100}}},
            {20, 20, {{99, 200}}},
            {10, 10, {{99, 100}}},
            {20, 20, {{99, 200}}}}},
          {"metric4",
           {{30, 30, {{99, 300}}},
            {40, 40, {{99, 400}}},
            {30, 30, {{99, 300}}},
            {40, 40, {{99, 400}}}}}}},
        {"table2",
         {{"metric3",
           {{10, 10, {{99, 200}}},
            {20, 20, {{99, 300}}},
            {10, 10, {{99, 200}}},
            {20, 20, {{99, 300}}}}},
          {"metric4",
           {{40, 40, {{99, 300}}},
            {50, 50, {{99, 400}}},
            {40, 40, {{99, 300}}},
            {50, 50, {{99, 400}}}}}}},
        {"table3",
         {{"metric3",
           {{10, 10, {{99, 200}}},
            {20, 20, {{99, 300}}}}},
          {"metric4",
           {{40, 40, {{99, 300}}},
            {50, 50, {{99, 400}}}}}}}
    }));
  }
}

TEST(TestMetricsCollector, TestMergeToClusterLevelMetrics) {
  // Merge empty metrics.
  {
    MetricsCollector::TablesMetrics tables_metrics;
    MetricsCollector::TablesHistMetrics tables_hist_metrics;
    MetricsCollector::Metrics cluster_metrics;
    ASSERT_OK(MetricsCollector::MergeToClusterLevelMetrics(tables_metrics, tables_hist_metrics,
                                                           &cluster_metrics));
    ASSERT_TRUE(cluster_metrics.empty());
  }
  // Merge multi metrics.
  {
    MetricsCollector::TablesMetrics tables_metrics({
        {"table1",
         {{"metric1", 100}}},
        {"table2",
         {{"metric1", 10},
          {"metric2", 20}}},
        {"table3",
         {{"metric1", 1},
          {"metric2", 2},
          {"metric3", 3}}}
    });
    MetricsCollector::TablesHistMetrics tables_hist_metrics;  // TODO(yingchun) not used now.
    MetricsCollector::Metrics cluster_metrics({{"metric2", 0}});
    ASSERT_OK(MetricsCollector::MergeToClusterLevelMetrics(tables_metrics, tables_hist_metrics,
                                                           &cluster_metrics));
    ASSERT_EQ(cluster_metrics, MetricsCollector::Metrics({{"metric2", 22}}));
  }
}

TEST(TestMetricsCollector, TestParseMetrics) {
  auto collector = BuildCollector();
  collector->metrics_dict_.metric_type_ = {
      {"server_metric", MetricsCollector::MetricType::kCounter},
      {"metric_counter1", MetricsCollector::MetricType::kCounter},
      {"metric_counter2", MetricsCollector::MetricType::kCounter},
      {"average_metric", MetricsCollector::MetricType::kMeanSummary},
      {"server_metric_histogram", MetricsCollector::MetricType::kSummary},
      {"metric_histogram1", MetricsCollector::MetricType::kSummary},
      {"metric_histogram2", MetricsCollector::MetricType::kSummary}
  };
  string data(R"*(
[
  {
    "type": "server",
    "id": "kudu.tabletserver",
    "attributes": {
      "attrA": "val1",
      "attrB": "val2"
    },
    "metrics": [
      {
        "name": "server_metric",
        "value": 123
      },
      {
        "name": "server_metric_histogram",
        "total_count": 60,
        "min": 4,
        "mean": 76.16666666666667,
        "percentile_75": 25,
        "percentile_95": 66,
        "percentile_99": 79,
        "percentile_99_9": 3486,
        "percentile_99_99": 3486,
        "max": 3486,
        "total_sum": 4570
      }
    ]
  },
  {
    "type": "table",
    "id": "table1",
    "attributes": {
      "attr1": "val2",
      "attr2": "val3"
    },
    "metrics": [
      {
        "name": "metric_counter1",
        "value": 10
      },
      {
        "name": "metric_counter2",
        "value": 20
      },
      {
        "name": "average_metric",
        "value": 1,
        "total_sum": 10,
        "total_count": 10
      },
      {
        "name": "metric_histogram1",
        "total_count": 17,
        "min": 6,
        "mean": 47.8235,
        "percentile_75": 62,
        "percentile_95": 72,
        "percentile_99": 73,
        "percentile_99_9": 73,
        "percentile_99_99": 73,
        "max": 73,
        "total_sum": 813
      }
    ]
  },
  {
    "type": "table",
    "id": "table2",
    "attributes": {
      "attr1": "val3",
      "attr2": "val2"
    },
    "metrics": [
      {
        "name": "metric_counter1",
        "value": 100
      },
      {
        "name": "average_metric",
        "value": 0.5,
        "total_sum": 20,
        "total_count": 20
      },
      {
        "name": "metric_histogram1",
        "total_count": 170,
        "min": 60,
        "mean": 478.235,
        "percentile_75": 620,
        "percentile_95": 720,
        "percentile_99": 730,
        "percentile_99_9": 735,
        "percentile_99_99": 735,
        "max": 735,
        "total_sum": 8130
      },
      {
        "name": "metric_histogram2",
        "total_count": 34,
        "min": 6,
        "mean": 47.8235,
        "percentile_75": 62,
        "percentile_95": 72,
        "percentile_99": 72,
        "percentile_99_9": 73,
        "percentile_99_99": 73,
        "max": 73,
        "total_sum": 813
      }
    ]
  }
])*");

  MetricsCollector::TablesMetrics tables_metrics;
  MetricsCollector::TablesHistMetrics tables_hist_metrics;
  MetricsCollector::Metrics host_metrics;
  MetricsCollector::HistMetrics host_hist_metrics;
  ASSERT_OK(collector->ParseMetrics(MetricsCollector::NodeType::kTServer,
                                    data,
                                    &tables_metrics, &host_metrics,
                                    &tables_hist_metrics, &host_hist_metrics));
  ASSERT_EQ(tables_metrics, MetricsCollector::TablesMetrics({
      {"table1",
       {{"metric_counter1", 10},
        {"metric_counter2", 20}}},
      {"table2",
       {{"metric_counter1", 100}}}
  }));
  EXPECT_EQ(tables_hist_metrics, MetricsCollector::TablesHistMetrics({
      {"table1",
       {{"average_metric",
         {{10, 10, {{50, 1}}}}},
        {"metric_histogram1",
         {{17, 813, {{75, 62},
                     {95, 72},
                     {99, 73}}}}}}},
      {"table2",
       {{"average_metric",
         {{20, 20, {{50, 0.5}}}}},
        {"metric_histogram1",
         {{170, 8130, {{75, 620},
                       {95, 720},
                       {99, 730}}}}},
        {"metric_histogram2",
         {{34, 813, {{75, 62},
                     {95, 72},
                     {99, 72}}}}}}}
  }));
  ASSERT_EQ(host_metrics, MetricsCollector::Metrics({
      {"metric_counter1", 110},
      {"metric_counter2", 20},
      {"server_metric", 123}
  }));
  EXPECT_EQ(host_hist_metrics, MetricsCollector::HistMetrics({
      {"average_metric",
       {{10, 10, {{50, 1}}},
        {20, 20, {{50, 0.5}}}}},
      {"metric_histogram1",
       {{17, 813, {{75, 62},
                   {95, 72},
                   {99, 73}}},
        {170, 8130, {{75, 620},
                     {95, 720},
                     {99, 730}}}}},
      {"metric_histogram2",
       {{34, 813, {{75, 62},
                   {95, 72},
                   {99, 72}}}}},
      {"server_metric_histogram",
       {{60, 4570, {{75, 25},
                    {95, 66},
                    {99, 79}}}}}
  }));
}

TEST(TestMetricsCollector, TestInitMetrics) {
  FLAGS_collector_metrics_types_for_testing = R"*(
[
  {
    "type": "table",
    "id": "table1",
    "metrics": [
      {
        "name": "counter_metric1",
        "type": "counter",
        "label": "counter_metric1 lable",
        "unit": "counter_metric1 unit",
        "description": "counter_metric1 description",
        "level": "warn",
        "value": 0
      },
      {
        "name": "histogram_metric1",
        "type": "histogram",
        "label": "histogram_metric1 lable",
        "unit": "histogram_metric1 unit",
        "description": "histogram_metric1 description",
        "level": "warn",
        "value": 0
      },
      {
        "name": "gauge_metric1",
        "type": "gauge",
        "label": "gauge_metric1 lable",
        "unit": "gauge_metric1 unit",
        "description": "gauge_metric1 description",
        "level": "warn",
        "value": 0
      }
    ]
  },
  {
    "type": "server",
    "metrics": [
      {
        "name": "counter_metric2",
        "type": "counter",
        "label": "counter_metric2 lable",
        "unit": "counter_metric2 unit",
        "description": "counter_metric2 description",
        "level": "warn",
        "value": 0
      },
      {
        "name": "histogram_metric2",
        "type": "histogram",
        "label": "histogram_metric2 lable",
        "unit": "histogram_metric2 unit",
        "description": "histogram_metric2 description",
        "level": "warn",
        "value": 0
      },
      {
        "name": "gauge_metric2",
        "type": "gauge",
        "label": "gauge_metric2 lable",
        "unit": "gauge_metric2 unit",
        "description": "gauge_metric2 description",
        "level": "warn",
        "value": 0
      }
    ]
  }
])*";
  auto collector = BuildCollector();
  ASSERT_OK(collector->InitMetrics());
  std::unordered_map<std::string, MetricsCollector::MetricType> expect_metric_types({
    {"counter_metric1", MetricsCollector::MetricType::kCounter},
    {"histogram_metric1", MetricsCollector::MetricType::kSummary},
    {"gauge_metric1", MetricsCollector::MetricType::kGauge},
    {"counter_metric2", MetricsCollector::MetricType::kCounter},
    {"histogram_metric2", MetricsCollector::MetricType::kSummary},
    {"gauge_metric2", MetricsCollector::MetricType::kGauge},
    {"live_row_count", MetricsCollector::MetricType::kGauge},
    {"merged_entities_count_of_tablet", MetricsCollector::MetricType::kGauge}
  });
  ASSERT_EQ(expect_metric_types, collector->metrics_dict_.metric_type_);
}

TEST(TestMetricsCollector, TestInitFilters) {
  FLAGS_collector_attributes_filter = "attr1:val1,val2;attr2:val1";
  auto collector = BuildCollector();
  collector->InitFilters();
  unordered_map<string, set<string>> expect_attributes_filter({
      {"attr1",
       {"val1", "val2"}},
      {"attr2",
       {"val1"}}
  });
  ASSERT_EQ(collector->attributes_filter_, expect_attributes_filter);
}

#define CHECK_URL_PARAMETERS(metrics, attributes, expect_url)                                     \
do {                                                                                              \
  FLAGS_collector_metrics = metrics;                                                              \
  FLAGS_collector_attributes_filter = attributes;                                                 \
  auto collector = BuildCollector();                                                              \
  collector->InitFilters();                                                                       \
  collector->InitMetricsUrlParameters();                                                          \
  ASSERT_EQ(collector->metric_url_parameters_, expect_url);                                       \
} while (false)

TEST(TestMetricsCollector, TestInitMetricsUrlParameters) {
  CHECK_URL_PARAMETERS("", "",
                       "/metrics?compact=1&merge_rules=tablet|table|table_name");
  CHECK_URL_PARAMETERS("m1,m2,m3", "",
                       "/metrics?compact=1&metrics=m1,m2,m3&merge_rules=tablet|table|table_name");
  CHECK_URL_PARAMETERS("", "attr1:a1,a2;attr2:a3",
                       "/metrics?compact=1&merge_rules=tablet|table|table_name"
                       "&attributes=attr2,a3,attr1,a1,attr1,a2,");
  CHECK_URL_PARAMETERS("", "",
                       "/metrics?compact=1&merge_rules=tablet|table|table_name");
}

TEST(TestMetricsCollector, TestInitClusterLevelMetrics) {
  FLAGS_collector_cluster_level_metrics = "m1,m2,m3";
  auto collector = BuildCollector();
  collector->InitClusterLevelMetrics();
  MetricsCollector::Metrics cluster_metrics({
      {"m1", 0},
      {"m2", 0},
      {"m3", 0},
  });
  ASSERT_EQ(collector->cluster_metrics_filter_, cluster_metrics);
}
}  // namespace collector
}  // namespace kudu

