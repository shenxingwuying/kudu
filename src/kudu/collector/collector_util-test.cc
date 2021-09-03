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

#include "kudu/collector/collector_util.h"

#include <map>
#include <string>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"

DECLARE_bool(collector_simplify_hostname);
DECLARE_double(collector_summary_quantile_error);

using std::map;
using std::string;

namespace kudu {
namespace collector {

TEST(TestCollectorUtil, TestExtractHostName) {
  FLAGS_collector_simplify_hostname = false;
  ASSERT_EQ(ExtractHostName("1.2.3.4:5555"), "1.2.3.4");
  ASSERT_EQ(ExtractHostName("host-name.bj:5555"), "host-name.bj");
  ASSERT_EQ(ExtractHostName("1.2.3.4"), "1.2.3.4");
  ASSERT_EQ(ExtractHostName("host-name.bj"), "host-name.bj");

  FLAGS_collector_simplify_hostname = true;
  ASSERT_EQ(ExtractHostName("1.2.3.4:5555"), "1");
  ASSERT_EQ(ExtractHostName("host-name.bj:5555"), "host-name");
  ASSERT_EQ(ExtractHostName("1.2.3.4"), "1");
  ASSERT_EQ(ExtractHostName("host-name.bj"), "host-name");
}

TEST(TestCollectorUtil, TestGetQuantiles) {
  const std::vector<double> expect_quantiles({0.75, 0.95, 0.99});
  auto quantiles = GetQuantiles();
  ASSERT_EQ(expect_quantiles.size(), quantiles.size());
  int i = 0;
  for (const auto& quantile : quantiles) {
    ASSERT_NEAR(expect_quantiles[i], quantile.quantile, 1e-6);
    ASSERT_NEAR(quantile.error, FLAGS_collector_summary_quantile_error, 1e-6);
    ++i;
  }
}

TEST(TestCollectorUtil, TestGetPercentiles) {
  const map<string, int> expect_percentiles({{"percentile_75", 75},
                                             {"percentile_95", 95},
                                             {"percentile_99", 99}});
  auto percentiles = GetPercentiles();
  ASSERT_EQ(expect_percentiles.size(), percentiles.size());
  for (const auto& percentile : percentiles) {
    auto found = FindOrDie(expect_percentiles, percentile.first);
    ASSERT_EQ(found, percentile.second);
  }
}

}  // namespace collector
}  // namespace kudu

