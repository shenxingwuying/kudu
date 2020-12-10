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

#include <cmath>
#include <cstddef>
#include <set>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"

DEFINE_bool(collector_simplify_hostname, true,
            "Whether to use simplify hostname, i.e. the first section of hostname.");
DEFINE_string(collector_summary_quantile_values, "0.75,0.95,0.99",
              "Quantile values of summary type metrics.");
DEFINE_double(collector_summary_quantile_error, 0.01,
              "Quantile error of summary type metrics.");

DECLARE_string(collector_report_method);

using std::map;
using std::set;
using std::string;
using std::vector;
using strings::Split;
using strings::Substitute;

DEFINE_validator(collector_summary_quantile_values,
                 [] (const char* flag_name, const string& value) {
  vector<string> summary_quantile_values_vec = Split(value, ",", strings::SkipEmpty());
  set<string> summary_quantile_values(summary_quantile_values_vec.begin(),
                                      summary_quantile_values_vec.end());
  if (summary_quantile_values.size() != summary_quantile_values_vec.size()) {
    LOG(ERROR) << Substitute("There are duplicate values in --$0=$1.", flag_name, value);
    return false;
  }

  for (const auto& summary_quantile_value : summary_quantile_values) {
    double value;
    if (!safe_strtod(summary_quantile_value, &value)) {
      LOG(ERROR) << Substitute("Unable to parse value($0) to double in --$1=$2.",
                               summary_quantile_value, flag_name, value);
      return false;
    }
  }

  return true;
});

DEFINE_validator(collector_summary_quantile_error,
                 [] (const char* flag_name, double value) {
  if (value < 0 || value > 1) {
    LOG(ERROR) << Substitute("--$0 should in range [0.0, 1.0], but got $1.", flag_name, value);
    return false;
  }

  return true;
});

namespace kudu {
namespace collector {

string ExtractHostName(const string& url) {
  string hostname(url);
  size_t pos = hostname.find(':');
  if (pos != string::npos) {
    hostname = hostname.substr(0, pos);
  }

  if (FLAGS_collector_simplify_hostname) {
    pos = hostname.find('.');
    if (pos != string::npos) {
      hostname = hostname.substr(0, pos);
    }
  }

  return hostname;
}

bool RunOnceMode() {
  static bool run_once = (FLAGS_collector_report_method == "once");
  return run_once;
}

prometheus::Summary::Quantiles GetQuantiles() {
  static prometheus::Summary::Quantiles quantiles;
  static std::once_flag once;
  std::call_once(once, [&]() {
    vector<string> summary_quantile_values_vec =
        Split(FLAGS_collector_summary_quantile_values, ",", strings::SkipEmpty());
    set<string> summary_quantile_values(summary_quantile_values_vec.begin(),
                                        summary_quantile_values_vec.end());
    CHECK_EQ(summary_quantile_values.size(), summary_quantile_values_vec.size());
    for (const auto& summary_quantile_value : summary_quantile_values) {
      double value;
      CHECK(safe_strtod(summary_quantile_value, &value));
      quantiles.push_back({value, FLAGS_collector_summary_quantile_error});
    }
  });

  return quantiles;
}

map<string, int> GetPercentiles() {
  static map<string, int> percentiles;
  static std::once_flag once;
  std::call_once(once, [&]() {
    vector<string> str_percentiles =
        Split(FLAGS_collector_summary_quantile_values, ",", strings::SkipEmpty());
    for (const auto& str_percentile : str_percentiles) {
      double value;
      CHECK(safe_strtod(str_percentile, &value));
      uint32_t percentile = std::lround(value * 100);
      percentiles.emplace(std::make_pair(Substitute("percentile_$0", percentile), percentile));
    }
  });

  return percentiles;
}


} // namespace collector
} // namespace kudu
