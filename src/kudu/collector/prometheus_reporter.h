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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest_prod.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/faststring.h"
#include "kudu/util/status.h"

namespace kudu {
namespace collector {

class PrometheusReporter {
 public:
  ~PrometheusReporter();

  static PrometheusReporter* instance() {
    static PrometheusReporter instance;
    return &instance;
  }

  prometheus::Registry& registry();

  static faststring exportOnce();

 private:
  PrometheusReporter();

  prometheus::Exposer exposer_;
  std::shared_ptr<prometheus::Registry> registry_;

  DISALLOW_COPY_AND_ASSIGN(PrometheusReporter);
};
} // namespace collector
} // namespace kudu