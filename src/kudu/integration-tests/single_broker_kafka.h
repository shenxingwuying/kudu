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

#include <string>

#include <glog/logging.h>
#include <kudu/gutil/strings/substitute.h>
#include <kudu/util/env.h>
#include <kudu/util/path_util.h>
#include <kudu/util/subprocess.h>

namespace kudu {
namespace duplication {
namespace kafka {

using std::string;
using strings::Substitute;

constexpr int kKafkaBasePort = 9092;

class SingleBrokerKafka {
 public:
  static const int kZookeeperSessionTimeoutS = 10;

  explicit SingleBrokerKafka(int offset_port) : offset_port_(offset_port) {
    string exe_file;
    CHECK_OK(Env::Default()->GetExecutablePath(&exe_file));
    // The path should be ${kudu_code_path}/build/latest/bin/testdata/kafka-simple-control.sh
    command_ =
        Substitute("$0", JoinPathSegments(DirName(exe_file), "testdata/kafka-simple-control.sh"));
  }

  bool InstallKafka(const string& keytab_path = "", const string& principal = "") {
    string command =
        Substitute("$0 install $1 $2 $3", command_, offset_port_, keytab_path, principal);
    Status status = Subprocess::Call({"/bin/bash", "-c", command});
    return status.ok();
  }

  bool StartBrokers() {
    string command = Substitute("$0 start $1", command_, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", command});
    return status.ok();
  }

  bool Alive() {
    string command = Substitute("$0 alive $1", command_, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", command});
    return status.ok();
  }

  bool StopBrokers() {
    string command = Substitute("$0 stop $1", command_, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", command});
    return status.ok();
  }

  bool UninstallKafka() {
    string command = Substitute("$0 uninstall $1", command_, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", command});
    return status.ok();
  }

 private:
  string command_;
  int offset_port_ = 0;
};

}  // namespace kafka
}  // namespace duplication
}  // namespace kudu
