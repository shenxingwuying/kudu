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

#include <glog/logging.h>

#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_util.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu {
namespace duplicator {

// If the wal file(segment) is writing(append-only), It has no footer. (In fact,
// segment structure can rebuild footer in-memory by scanning the segment)
// If the wal file is read-only, it has a footer.
class LogSegmentReader {
 public:
  explicit LogSegmentReader(const scoped_refptr<log::ReadableLogSegment>& segment)
      : segment_(segment), reader_(new log::LogEntryReader(segment.get())) {
    CHECK(segment_->HasFooter());
    min_replicate_index_ = segment_->footer().min_replicate_index();
    max_replicate_index_ = segment_->footer().max_replicate_index();
  }

  ~LogSegmentReader() = default;

  bool IsExists(int64_t opid_index) const;

  log::LogEntryPB* Next();

  const log::LogEntryPB* Value() const { return &current_entry_; }

 private:
  const scoped_refptr<log::ReadableLogSegment> segment_;
  std::unique_ptr<log::LogEntryReader> reader_;
  log::LogEntryPB current_entry_;

  int64_t min_replicate_index_ = -1;
  int64_t max_replicate_index_ = -1;
};

}  // namespace duplicator
}  // namespace kudu
