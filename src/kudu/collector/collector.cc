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

#include "kudu/collector/collector.h"

#include <ostream>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/collector/collector_util.h"
#include "kudu/collector/metrics_collector.h"
#include "kudu/collector/nodes_checker.h"
#include "kudu/collector/prometheus_reporter.h"
#include "kudu/collector/service_monitor.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/init.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_string(collector_cluster_name, "test",
              "Cluster name of this collector to operate, it is use for monitor system labels");
DEFINE_string(collector_master_addrs, "localhost",
              "Comma-separated list of Kudu master addresses where each address is of "
              "form 'hostname:port");
DEFINE_uint32(collector_metrics_collect_interval_sec, 60,
              "Number of interval seconds to collect metrics");
DEFINE_string(collector_report_method, "prometheus",
              "Which monitor system the metrics reported to. Now supported "
              "system: once, prometheus");
DEFINE_uint32(collector_timeout_sec, 10,
              "Number of seconds to wait for a master, tserver, or CLI tool to return metrics");
DEFINE_uint32(collector_warn_threshold_ms, 1000,
              "If a task takes more than this number of milliseconds, issue a warning with a "
              "trace");

using std::string;
using strings::Substitute;

namespace kudu {
namespace collector {

bool ValidateCollectorFlags() {
  if (FLAGS_collector_metrics_collect_interval_sec < 10 ||
      FLAGS_collector_metrics_collect_interval_sec > 60) {
    LOG(ERROR) << Substitute(
        "--collector_metrics_collect_interval_sec should be in range [10, 60]");
    return false;
  }

  if (FLAGS_collector_timeout_sec < 1 ||
      FLAGS_collector_timeout_sec >= FLAGS_collector_metrics_collect_interval_sec) {
    LOG(ERROR) << Substitute("--collector_timeout_sec should in range [1, $0)",
                             FLAGS_collector_metrics_collect_interval_sec);
    return false;
  }

  if (FLAGS_collector_report_method != "once" &&
      FLAGS_collector_report_method != "prometheus") {
    LOG(ERROR) << Substitute("--collector_report_method only support 'once' and 'prometheus'.");
    return false;
  }

  if (FLAGS_collector_cluster_name.empty() ||
      FLAGS_collector_master_addrs.empty()) {
    LOG(ERROR) << Substitute("--collector_cluster_name and --collector_master_addrs should "
                             "not be empty.");
    return false;
  }

  return true;
}

GROUP_FLAG_VALIDATOR(collector_flags, ValidateCollectorFlags);

Collector::Collector()
  : initialized_(false),
    stop_background_threads_latch_(1) {
}

Collector::~Collector() {
  Shutdown();
}

Status Collector::Init() {
  CHECK(!initialized_);

  nodes_checker_.reset(new NodesChecker());
  CHECK_OK(nodes_checker_->Init());
  metrics_collector_.reset(new MetricsCollector(nodes_checker_));
  CHECK_OK(metrics_collector_->Init());
  // In 'once' mode, some modules are not needed to init.
  if (!RunOnceMode()) {
    service_monitor_.reset(new ServiceMonitor());
    CHECK_OK(service_monitor_->Init());
  }

  initialized_ = true;
  return Status::OK();
}

Status Collector::Start() {
  CHECK(initialized_);

  google::FlushLogFiles(google::INFO); // Flush the startup messages.

  RETURN_NOT_OK(StartExcessLogFileDeleterThread());

  nodes_checker_->Start();
  metrics_collector_->Start();
  if (!RunOnceMode()) {
    service_monitor_->Start();
  }

  return Status::OK();
}

void Collector::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    metrics_collector_->Shutdown();
    nodes_checker_->Shutdown();
    if (RunOnceMode()) {
      std::cout << PrometheusReporter::instance()->exportOnce().data();
    } else {
      service_monitor_->Shutdown();
    }

    stop_background_threads_latch_.CountDown();
    if (excess_log_deleter_thread_) {
      excess_log_deleter_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

string Collector::ToString() {
  return "Collector";
}

Status Collector::StartExcessLogFileDeleterThread() {
  // Try synchronously deleting excess log files once at startup to make sure it
  // works, then start a background thread to continue deleting them in the
  // future.
  if (!FLAGS_logtostderr) {
    RETURN_NOT_OK_PREPEND(DeleteExcessLogFiles(Env::Default()),
                          "Unable to delete excess log files");
  }
  return Thread::Create("collector", "excess-log-deleter",
                        [this]() { this->ExcessLogFileDeleterThread(); },
                        &excess_log_deleter_thread_);
}

void Collector::ExcessLogFileDeleterThread() {
  // How often to attempt to clean up excess glog files.
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitFor(kWait)) {
    WARN_NOT_OK(DeleteExcessLogFiles(Env::Default()), "Unable to delete excess log files");
  }
}

} // namespace collector
} // namespace kudu
