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

#pragma once

#include <cstdint>
#include <memory>
#include "kudu/consensus/log_util.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu {
namespace duplicator {

// If the wal file(segment) is writing(append-only), It have no footer.
// If the wal file is read-only, it has footer.
class WalFileReader {
 public:
  explicit WalFileReader(const scoped_refptr<log::ReadableLogSegment>& segment)
      : segment_(segment) {
    reader_ = new log::LogEntryReader(segment.get());
    if (segment_->HasFooter()) {
      min_replicate_index_ = segment_->footer().has_min_replicate_index()
                                 ? segment_->footer().min_replicate_index()
                                 : INT64_MAX;
      max_replicate_index_ = segment_->footer().has_max_replicate_index()
                                 ? segment_->footer().max_replicate_index()
                                 : INT64_MIN;
    }
    file_size_ = segment_->file_size();
    segment_->file();
  }

  ~WalFileReader() { delete reader_; }

  Status IsExists(int64_t opid_index) {
    if (segment_->HasFooter()) {
      if (segment_->footer().has_min_replicate_index() &&
          segment_->footer().has_max_replicate_index()) {
        int64_t min_index = segment_->footer().min_replicate_index();
        int64_t max_index = segment_->footer().max_replicate_index();
        if (opid_index >= min_index && opid_index <= max_index) {
          return Status::OK();
        }
        return Status::NotFound("not found");
      }
    }
    return Status::EndOfFile("not found");
  }

  // Find the first Op of >= opid_index
  Status Seek(int64_t opid_index) {
    if (min_replicate_index_ < max_replicate_index_) {
      if (opid_index >= min_replicate_index_ && opid_index <= max_replicate_index_) {
        while (Next() != nullptr) {
          int64_t index = 0;
          if (current_entry_.has_replicate()) {
            index = current_entry_.replicate().id().index();
          } else if (current_entry_.has_commit() && current_entry_.commit().has_commited_op_id()) {
            index = current_entry_.commit().commited_op_id().index();
          } else {
            VLOG(1) << "impossible";
          }
          if (index == opid_index) {
            return Status::OK();
          }
          if (index < opid_index) {
            continue;
          }
          if (index == max_replicate_index_) finished_ = true;
          return Status::NotFound("not found at the wal file");
        }
      } else {
        return Status::NotFound("not found opid index");
      }
    } else {
    }
    return Status::OK();
  }

  log::LogEntryPB* Next() {
    std::unique_ptr<log::LogEntryPB> entry;
    Status s = reader_->ReadNextEntry(&entry);
    if (s.IsEndOfFile()) {
      finished_ = true;
      return nullptr;
    }
    current_entry_ = std::move(*entry);
    return &current_entry_;
  }

  log::LogEntryPB* Value() { return &current_entry_; }

  bool Refresh() {
    // TODO(duyuqi), test the writable file.
    if (segment_->file_size() > file_size_) {
      // Need refresh.
      int offset_ = reader_->offset();
    }
    log::LogEntryReader* reader = new log::LogEntryReader(segment_.get());
    reader->read_up_to_offset();
    delete reader;
    reader->offset();
    delete reader_;

    reader_ = new log::LogEntryReader(segment_.get());
    return true;
  }

  bool IsFinished() { return finished_; }

 private:
  const scoped_refptr<log::ReadableLogSegment> segment_;
  log::LogEntryReader* reader_;
  log::LogEntryPB current_entry_;
  ssize_t file_size_;
  bool finished_;

  int64_t min_replicate_index_;
  int64_t max_replicate_index_;
};

}  // namespace duplicator
}  // namespace kudu
