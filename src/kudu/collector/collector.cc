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

#include <cstdio>
#include <cstdlib>

#include <functional>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/collector/collector_util.h"
#include "kudu/collector/metrics_collector.h"
#include "kudu/collector/nodes_checker.h"
#include "kudu/collector/prometheus_reporter.h"
#include "kudu/collector/service_monitor.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
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
DEFINE_uint32(collector_do_kinit_interval_sec, 60 * 60,
              "Number of interval seconds to do kinit operation.");

DECLARE_string(keytab_file);
DECLARE_string(principal);

using std::string;
using std::to_string;
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

void Collector::RenewThread() {
  const MonoDelta kWait = MonoDelta::FromSeconds(FLAGS_collector_do_kinit_interval_sec);
  while (!stop_background_threads_latch_.WaitFor(kWait)) {
    // try renew first
    string renew_cmd = Substitute("kinit -R $0", FLAGS_principal);
    CHECK_OK(Subprocess::Call({renew_cmd}));
    string renew_result;
    CHECK_OK(Subprocess::Call({"klist"}, "", &renew_result, nullptr));
    size_t pos = renew_result.find(FLAGS_principal);
    if (pos == string::npos) {
      // if fail then do kinit
      string kinit_cmd = Substitute("kinit -kt $0 $1", FLAGS_keytab_file, FLAGS_principal);
      CHECK_OK(Subprocess::Call({kinit_cmd}));
    }

    string stdout;
    CHECK_OK(Subprocess::Call({"klist"}, "", &stdout, nullptr));
    LOG(INFO) << "After RenewThread renew, principals : " << stdout;
  }
}

Collector::Collector()
  : initialized_(false),
    stop_background_threads_latch_(1) {
}

Collector::~Collector() {
  Shutdown();
}

static const char *kCollectorKrb5CCNamePrefix = "/tmp/krb5cc_kudu_collector";

Status Collector::Init() {
  CHECK(!initialized_);

  if (!FLAGS_keytab_file.empty()) {
    string kKrb5CCName = kCollectorKrb5CCNamePrefix + to_string(rand() % 9000 + 1000);
    setenv("KRB5CCNAME", kKrb5CCName.c_str(), 1);
    string kinit_cmd = Substitute("kinit -kt $0 $1", FLAGS_keytab_file, FLAGS_principal);
    RETURN_NOT_OK(Subprocess::Call({kinit_cmd}));
    string stdout;
    RETURN_NOT_OK(Subprocess::Call({"klist"}, "", &stdout, nullptr));
    LOG(INFO) << "Collector kinit cmd :" << kinit_cmd
              << ", after kinit, principals : " << stdout;
  }

  nodes_checker_.reset(new NodesChecker());
  CHECK_OK(nodes_checker_->Init());
  metrics_collector_.reset(new MetricsCollector(nodes_checker_));
  CHECK_OK(metrics_collector_->Init());
  // In 'once' mode, some modules are not needed to init.
  if (!RunOnceMode()) {
    service_monitor_.reset(new ServiceMonitor());
    CHECK_OK(service_monitor_->Init());
  }

  if (!FLAGS_keytab_file.empty()) {
    // Start the renew thread.
    RETURN_NOT_OK(Thread::Create("collector", "renew-thread",
                                 [this]() { this->RenewThread(); },
                                 &renew_thread_));
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
      std::cout << PrometheusReporter::exportOnce().data();
    } else {
      service_monitor_->Shutdown();
    }

    stop_background_threads_latch_.CountDown();
    if (excess_log_deleter_thread_) {
      excess_log_deleter_thread_->Join();
    }
    if (renew_thread_) {
      renew_thread_->Join();
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
