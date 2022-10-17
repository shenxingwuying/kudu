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
#include <kudu/util/subprocess.h>

namespace kudu {
namespace duplication {
namespace kafka {

using strings::Substitute;

constexpr int kKafkaBasePort = 9092;
constexpr const char* kControlCommand = "sh bin/testdata/kafka-simple-control.sh";

class SingleBrokerKafka {
 public:
  explicit SingleBrokerKafka(int offset_port) : offset_port_(offset_port) {}

  void InitKafka() {
    std::string start_cmd = Substitute("$0 start $1", kControlCommand, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", start_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "start kafka failed";
    }
    LOG(INFO) << "init kafka success, cmd: " << start_cmd;
  }

  void StartKafka() {
    std::string start_cmd = Substitute("$0 start_only $1", kControlCommand, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", start_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "start kafka failed";
    }
    LOG(INFO) << "start kafka success, cmd: " << start_cmd;
  }

  void StopKafka() {
    std::string stop_cmd = Substitute("$0 stop_only $1", kControlCommand, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", stop_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "start kafka failed";
    }
    LOG(INFO) << "stop kafka success, cmd: " << stop_cmd;
  }

  void DestroyKafka() {
    std::string stop_cmd = Substitute("$0 stop $1", kControlCommand, offset_port_);
    Status status = Subprocess::Call({"/bin/bash", "-c", stop_cmd});
    if (!status.ok()) {
      LOG(FATAL) << "stop kafka failed";
    }
    LOG(INFO) << "destroy kafka success, cmd: " << stop_cmd;
  }

 private:
  int offset_port_ = 0;
};

}  // namespace kafka
}  // namespace duplication
}  // namespace kudu
