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

#include "kudu/collector/prometheus_reporter.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

DEFINE_int32(collector_prometheus_exposer_port, 9050,
             "Prometheus expose http port on collector");

DECLARE_string(webserver_interface);

using prometheus::Registry;
using std::string;
using strings::Substitute;

namespace kudu {
namespace collector {

PrometheusReporter::PrometheusReporter()
  : exposer_(Substitute("$0:$1",
                        FLAGS_webserver_interface.empty() ? "0.0.0.0" : FLAGS_webserver_interface,
                        FLAGS_collector_prometheus_exposer_port)),
    registry_(std::make_shared<Registry>()) {
  exposer_.RegisterCollectable(registry_, "/metrics");
}

PrometheusReporter::~PrometheusReporter() {
}

Registry& PrometheusReporter::registry() {
  return *registry_;
}

faststring PrometheusReporter::exportOnce() const {
  EasyCurl curl;
  faststring dst;
  CHECK_OK(curl.FetchURL(Substitute("localhost:$0/metrics",
                                    FLAGS_collector_prometheus_exposer_port),
                         &dst));
  return dst;
}

} // namespace collector
} // namespace kudu
