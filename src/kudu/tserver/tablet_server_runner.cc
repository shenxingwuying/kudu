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

#include "kudu/tserver/tablet_server_runner.h"

#include <cstdint>
#include <iostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/version_info.h"

DEFINE_double(fault_before_start, 0.0,
              "Fake fault flag that always causes a crash on startup. "
              "Used to test the test infrastructure. Should never be set outside of tests.");
TAG_FLAG(fault_before_start, hidden);
TAG_FLAG(fault_before_start, unsafe);

DECLARE_uint32(tablet_apply_pool_overload_threshold_ms);

using gflags::SET_FLAGS_DEFAULT;
using std::string;
using std::to_string;

namespace kudu {
namespace tserver {

void SetTabletServerFlagDefaults() {
  constexpr int32_t kDefaultNumServiceThreads = 20;

  // Reset some default values before parsing gflags.
  CHECK_NE("", SetCommandLineOptionWithMode(
      "rpc_bind_addresses",
      strings::Substitute("0.0.0.0:$0", TabletServer::kDefaultPort).c_str(),
      SET_FLAGS_DEFAULT));
  CHECK_NE("", SetCommandLineOptionWithMode(
      "rpc_num_service_threads",
      to_string(kDefaultNumServiceThreads).c_str(),
      SET_FLAGS_DEFAULT));
  CHECK_NE("", SetCommandLineOptionWithMode(
      "webserver_port",
      to_string(TabletServer::kDefaultWebPort).c_str(),
      SET_FLAGS_DEFAULT));
  // Setting the default value of the 'force_block_cache_capacity' flag to
  // 'false' makes the corresponding group validator enforce proper settings
  // for the memory limit and the cfile cache capacity.
  CHECK_NE("", SetCommandLineOptionWithMode(
      "force_block_cache_capacity",
      "false",
       SET_FLAGS_DEFAULT));
}

Status RunTabletServer() {
  string nondefault_flags = GetNonDefaultFlags();
  LOG(INFO) << "Tablet server non-default flags:\n"
            << nondefault_flags << '\n'
            << "Tablet server version:\n"
            << VersionInfo::GetAllVersionInfo();

  g_tablet_server = new TabletServer({});
  RETURN_NOT_OK(g_tablet_server->Init());
  MAYBE_FAULT(FLAGS_fault_before_start);
  RETURN_NOT_OK(g_tablet_server->Start());

  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }
  delete g_tablet_server;
  g_tablet_server = nullptr;
}

} // namespace tserver
} // namespace kudu
