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

  explicit SingleBrokerKafka(int offset_port) : command_(), offset_port_(offset_port) {
    string exe_file;
    CHECK_OK(Env::Default()->GetExecutablePath(&exe_file));
    // The path should be ${kudu_code_path}/build/latest/bin/testdata/kafka-simple-control.sh
    command_ = Substitute("sh $0",
                          JoinPathSegments(DirName(exe_file), "testdata/kafka-simple-control.sh"));
    VLOG(0) << "Kafka Command: " << command_;
  }

  void InitKafka() {
    string start_cmd = Substitute("$0 start $1", command_, offset_port_);
    LOG(INFO) << "Init kafka cmd: " << start_cmd;
    Status status = Subprocess::Call({"/bin/bash", "-c", start_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "start kafka failed";
    }
    LOG(INFO) << "Init kafka success, cmd: " << start_cmd;
  }

  void StartKafka() {
    string start_cmd = Substitute("$0 start_only $1", command_, offset_port_);
    LOG(INFO) << "start kafka cmd: " << start_cmd;
    Status status = Subprocess::Call({"/bin/bash", "-c", start_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "start kafka failed";
    }
    LOG(INFO) << "start kafka success, cmd: " << start_cmd;
  }

  void StopKafka() {
    string stop_cmd = Substitute("$0 stop_only $1", command_, offset_port_);
    LOG(INFO) << "stop kafka cmd: " << stop_cmd;
    Status status = Subprocess::Call({"/bin/bash", "-c", stop_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "start kafka failed";
    }
    LOG(INFO) << "stop kafka success, cmd: " << stop_cmd;
  }

  void DestroyKafka() {
    string stop_cmd = Substitute("$0 stop $1", command_, offset_port_);
    LOG(INFO) << "destroy kafka cmd: " << stop_cmd;
    Status status = Subprocess::Call({"/bin/bash", "-c", stop_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "stop kafka failed";
    }
    LOG(INFO) << "destroy kafka success, cmd: " << stop_cmd;
  }

 private:
  string command_;
  int offset_port_ = 0;
};

}  // namespace kafka
}  // namespace duplication
}  // namespace kudu
