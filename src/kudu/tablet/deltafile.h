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

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/common/rowid.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/once.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class BlockId;
class ColumnBlock;
class FsManager;
class MemTracker;
class RowChangeList;
class ScanSpec;
class SelectionVector;
struct ColumnId;

namespace cfile {
class BinaryPlainBlockDecoder;
class IndexTreeIterator;
struct ReaderOptions;
} // namespace cfile

namespace fs {
class BlockCreationTransaction;
class ReadableBlock;
class WritableBlock;
struct IOContext;
} // namespace fs

namespace tablet {

class Mutation;
class MvccSnapshot;
struct RowIteratorOptions;

class DeltaFileWriter {
 public:
  // Construct a new delta file writer.
  //
  // The writer takes ownership of the block and will Close it in Finish().
  explicit DeltaFileWriter(std::unique_ptr<fs::WritableBlock> block);

  Status Start();

  // Closes the delta file, including the underlying writable block.
  // Returns Status::Aborted() if no deltas were ever appended to this
  // writer.
  Status Finish();

  // Closes the delta file, finalizing the underlying block and releasing
  // it to 'transaction'.
  //
  // Returns Status::Aborted() if no deltas were ever appended to this
  // writer.
  Status FinishAndReleaseBlock(fs::BlockCreationTransaction* transaction);

  // Append a given delta to the file. This must be called in ascending order
  // of (key, timestamp) for REDOS and ascending order of key, descending order
  // of timestamp for UNDOS.
  template<DeltaType Type>
  Status AppendDelta(const DeltaKey &key, const RowChangeList &delta);

  void WriteDeltaStats(std::unique_ptr<DeltaStats> stats);

  std::unique_ptr<DeltaStats> release_delta_stats() {
    return std::move(delta_stats_);
  }

  size_t written_size() const {
    return writer_->written_size();
  }

 private:
  Status DoAppendDelta(const DeltaKey &key, const RowChangeList &delta);

  std::unique_ptr<DeltaStats> delta_stats_;
  std::unique_ptr<cfile::CFileWriter> writer_;

  // Buffer used as a temporary for storing the serialized form
  // of the deltas
  faststring tmp_buf_;

  #ifndef NDEBUG
  // The index of the previously written row.
  // This is used in debug mode to make sure that rows are appended
  // in order.
  DeltaKey last_key_;
  bool has_appended_;
  #endif

  DISALLOW_COPY_AND_ASSIGN(DeltaFileWriter);
};

class DeltaFileReader : public DeltaStore,
                        public std::enable_shared_from_this<DeltaFileReader> {
 public:
  static const char * const kDeltaStatsEntryName;

  // Fully open a delta file using a previously opened block.
  //
  // After this call, the delta reader is safe for use.
  static Status Open(std::unique_ptr<fs::ReadableBlock> block,
                     DeltaType delta_type,
                     cfile::ReaderOptions options,
                     std::shared_ptr<DeltaFileReader>* reader_out);

  // Lazily opens a delta file using a previously opened block. A lazy open
  // does not incur additional I/O, nor does it validate the contents of
  // the delta file.
  //
  // Init() must be called before using the file's stats.
  static Status OpenNoInit(std::unique_ptr<fs::ReadableBlock> block,
                           DeltaType delta_type,
                           cfile::ReaderOptions options,
                           std::unique_ptr<DeltaStats> delta_stats,
                           std::shared_ptr<DeltaFileReader>* reader_out);

  Status Init(const fs::IOContext* io_context) override;

  bool Initted() const override {
    return init_once_.init_succeeded();
  }

  // See DeltaStore::NewDeltaIterator(...)
  Status NewDeltaIterator(const RowIteratorOptions& opts,
                          std::unique_ptr<DeltaIterator>* iterator) const override;

  // See DeltaStore::CheckRowDeleted
  Status CheckRowDeleted(rowid_t row_idx,
                         const fs::IOContext* io_context,
                         bool *deleted) const override;

  uint64_t EstimateSize() const override;

  const BlockId& block_id() const { return reader_->block_id(); }

  const DeltaStats& delta_stats() const override {
    std::lock_guard<simple_spinlock> l(stats_lock_);
    DCHECK(delta_stats_);
    return *delta_stats_;
  }

  bool has_delta_stats() const override {
    std::lock_guard<simple_spinlock> l(stats_lock_);
    return delta_stats_ != nullptr;
  }

  std::string ToString() const override {
    if (!init_once_.init_succeeded()) return reader_->ToString();
    return strings::Substitute("$0 ($1)", reader_->ToString(), delta_stats_->ToString());
  }

  // Returns true if this delta file may include any deltas which need to be
  // applied when scanning the given snapshots, or if the file has not yet
  // been fully initialized.
  bool IsRelevantForSnapshots(const std::optional<MvccSnapshot>& snap_to_exclude,
                              const MvccSnapshot& snap_to_include) const;

  // Clone this DeltaFileReader for testing and validation purposes (such as
  // while in DEBUG mode). The resulting object will not be Initted().
  Status CloneForDebugging(FsManager* fs_manager,
                           const std::shared_ptr<MemTracker>& parent_mem_tracker,
                           std::shared_ptr<DeltaFileReader>* out) const;

 private:
  template<DeltaType Type>
  friend class DeltaFileIterator;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileReader);

  const std::shared_ptr<cfile::CFileReader> &cfile_reader() const {
    return reader_;
  }

  DeltaFileReader(std::unique_ptr<cfile::CFileReader> cf_reader,
                  std::unique_ptr<DeltaStats> delta_stats,
                  DeltaType delta_type);

  // Callback used in 'init_once_' to initialize this delta file.
  Status InitOnce(const fs::IOContext* io_context);

  Status ReadDeltaStats();

  std::shared_ptr<cfile::CFileReader> reader_;

  // TODO(awong): it'd be nice to not heap-allocate this and other usages of
  // delta stats.
  mutable simple_spinlock stats_lock_;
  std::unique_ptr<DeltaStats> delta_stats_;

  // The type of this delta, i.e. UNDO or REDO.
  const DeltaType delta_type_;

  KuduOnceLambda init_once_;
};

struct PreparedDeltaBlock;

// Iterator over the deltas contained in a delta file.
//
// See DeltaIterator for details.
template <DeltaType Type>
class DeltaFileIterator : public DeltaIterator {
 public:
  Status Init(ScanSpec* spec) override;

  Status SeekToOrdinal(rowid_t idx) override;

  // PrepareBatch() will read forward all blocks from the deltafile
  // which overlap with the block being prepared, enqueueing them onto
  // the 'delta_blocks_' deque. The prepared blocks are then used to
  // actually apply deltas in ApplyUpdates().
  Status PrepareBatch(size_t nrows, int prepare_flags) override;

  Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                      const SelectionVector& filter) override;

  Status ApplyDeletes(SelectionVector* sel_vec) override;

  Status SelectDeltas(SelectedDeltas* deltas) override;

  Status CollectMutations(std::vector<Mutation*>*dst, Arena* arena) override;

  Status FilterColumnIdsAndCollectDeltas(const std::vector<ColumnId>& col_ids,
                                         std::vector<DeltaKeyAndUpdate>* out,
                                         Arena* arena) override;

  std::string ToString() const override;

  bool HasNext() const override;

  bool MayHaveDeltas() const override;

  int64_t deltas_selected() const override {
    return preparer_.deltas_selected();
  }

  void set_deltas_selected(int64_t deltas_selected) override {
    preparer_.set_deltas_selected(deltas_selected);
  }

 private:
  friend class DeltaFileReader;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileIterator);

  // The pointers in 'opts' and 'dfr' must remain valid for the lifetime of the iterator.
  DeltaFileIterator(std::shared_ptr<DeltaFileReader> dfr,
                    RowIteratorOptions opts);

  // Determine the row index of the first update in the block currently
  // pointed to by index_iter_.
  Status GetFirstRowIndexInCurrentBlock(rowid_t *idx);

  // Determine the last updated row index contained in the given decoded block.
  static Status GetLastRowIndexInDecodedBlock(
    const cfile::BinaryPlainBlockDecoder &dec, rowid_t *idx);

  // Read the current block of data from the current position in the file
  // onto the end of the delta_blocks_ queue.
  Status ReadCurrentBlockOntoQueue();

  Status AddDeltas(rowid_t start_row, rowid_t stop_row);

  // Log a FATAL error message about a bad delta.
  void FatalUnexpectedDelta(const DeltaKey &key, const Slice &deltas,
                            const std::string &msg);

  std::shared_ptr<DeltaFileReader> dfr_;

  DeltaPreparer<DeltaFilePreparerTraits<Type>> preparer_;

  std::unique_ptr<cfile::IndexTreeIterator> index_iter_;

  bool prepared_;
  bool exhausted_;
  bool initted_;

  // After PrepareBatch(), the set of delta blocks in the delta file
  // which correspond to prepared_block_.
  std::deque<std::unique_ptr<PreparedDeltaBlock>> delta_blocks_;

  // Temporary buffer used in seeking.
  faststring tmp_buf_;

  cfile::CFileReader::CacheControl cache_blocks_;
};


} // namespace tablet
} // namespace kudu
