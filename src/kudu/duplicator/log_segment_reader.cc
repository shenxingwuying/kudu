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
//
// The file is define a Duplicator interface api,
// if user need to duplicate data to another storage,
// user should implement the interface just like kafka below.

#include "kudu/duplicator/log_segment_reader.h"

#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace duplicator {

bool LogSegmentReader::IsExists(int64_t opid_index) const {
  CHECK(segment_->HasFooter());
  return (min_replicate_index_ >= 0 && opid_index >= min_replicate_index_ &&
          opid_index <= max_replicate_index_);
}

log::LogEntryPB* LogSegmentReader::Next() {
  std::unique_ptr<log::LogEntryPB> entry;
  Status s = reader_->ReadNextEntry(&entry);
  if (s.IsEndOfFile()) {
    return nullptr;
  }
  current_entry_ = std::move(*entry);
  return &current_entry_;
}

}  // namespace duplicator
}  // namespace kudu
