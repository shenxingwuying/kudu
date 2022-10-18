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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/vector.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/dir_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/string_case.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/threadpool.h"

DECLARE_bool(force);
DECLARE_bool(print_meta);
DECLARE_int32(log_container_sample_count);
DECLARE_string(columns);
DECLARE_string(format);
DECLARE_int32(num_threads);
DECLARE_uint64(fs_max_thread_count_per_data_dir);

DEFINE_bool(print_rows, true,
            "Print each row in the CFile");
DEFINE_string(uuid, "",
              "The uuid to use in the filesystem. "
              "If not provided, one is generated");
DEFINE_bool(repair, false,
            "Repair any inconsistencies in the filesystem.");

DEFINE_string(table_id, "",
              "Restrict output to a specific table by id");
DECLARE_string(table_name);
DEFINE_string(tablet_id, "",
              "Restrict output to a specific tablet");
DEFINE_string(metadata_op_type, "print",
              "Operation type of metadata. The possible value is: print, compact, merge");
static bool ValidateMetadataOpType(const char* /*flagname*/, const std::string& value) {
  if (value == "print" || value == "compact" || value == "merge") {
    return true;
  }
  return false;
}
DEFINE_validator(metadata_op_type, &ValidateMetadataOpType);
DEFINE_int64(rowset_id, -1,
             "Restrict output to a specific rowset");
DEFINE_int32(column_id, -1,
             "Restrict output to a specific column");
DEFINE_uint64(block_id, 0,
              "Restrict output to a specific block");
DEFINE_bool(h, true,
            "Pretty-print values in human-readable units");
// TODO(yingchun): Should add related metrics, then we can get a more reasonable value.
DEFINE_double(estimate_latency_ratio_of_remove_file_to_load_lbm_container, 0.1,
              "Estimate latency ratio of remove a file to load a LBM container");
DEFINE_double(estimate_latency_ratio_of_truncate_file_to_load_lbm_container, 0.1,
              "Estimate latency ratio of truncate a file to load a LBM container");
DEFINE_double(estimate_latency_ratio_of_repunch_block_to_load_lbm_container, 0.1,
              "Estimate latency ratio of repunch a block to load a LBM container");
DEFINE_double(estimate_latency_ratio_of_rewrite_lbm_metadata_to_load_lbm_container, 1,
              "Estimate latency ratio of rewrite a LBM metadata to a load LBM container");
DEFINE_double(estimate_latency_ratio_of_load_tablet_metadata_to_load_lbm_container, 2,
              "Estimate latency ratio of load a tablet metadata to load a LBM container");
DEFINE_double(estimate_latency_ratio_of_bootstrap_tablet_to_load_lbm_container, 80,
              "Estimate latency ratio of bootstrap a tablet to load a LBM container");
DEFINE_double(estimate_latency_ratio_of_start_tablet_to_load_lbm_container, 5,
              "Estimate latency ratio of start a tablet to load a LBM container");

namespace kudu {
namespace tools {

using cfile::CFileIterator;
using cfile::CFileReader;
using cfile::ReaderOptions;
using fs::BlockDeletionTransaction;
using fs::Dir;
using fs::UpdateInstanceBehavior;
using fs::FsReport;
using fs::LogBlockManager;
using fs::ReadableBlock;
using rapidjson::Value;
using std::atomic;
using std::cout;
using std::endl;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;
using tablet::RowSetMetadata;
using tablet::TabletDataState;
using tablet::TabletMetadata;

namespace {

const char* kMetadataCompactOp = "compact";
const char* kMetadataMergeOp = "merge";
const char* kMetadataPrintOp = "print";

Status Check(const RunnerContext& /*context*/) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = !FLAGS_repair;
  fs_opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  FsReport report;
  RETURN_NOT_OK(fs_manager.Open(&report));

  // Stop now if we've already found a fatal error. Otherwise, continue;
  // we'll modify the report with our own check results and print it fully
  // at the end.
  if (report.HasFatalErrors()) {
    RETURN_NOT_OK(report.PrintAndCheckForFatalErrors());
  }

  // Get the "live" block IDs (i.e. those referenced by a tablet).
  BlockIdContainer live_block_ids;
  unordered_map<BlockId, string, BlockIdHash, BlockIdEqual> live_block_id_to_tablet;
  vector<string> tablet_ids;
  RETURN_NOT_OK(fs_manager.ListTabletIds(&tablet_ids));
  for (const auto& t : tablet_ids) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(TabletMetadata::Load(&fs_manager, t, &meta));
    BlockIdContainer tablet_live_block_ids = meta->CollectBlockIds();
    live_block_ids.insert(live_block_ids.end(),
                          tablet_live_block_ids.begin(),
                          tablet_live_block_ids.end());
    for (const auto& id : tablet_live_block_ids) {
      InsertOrDie(&live_block_id_to_tablet, id, t);
    }
  }

  // Get all of the block IDs reachable by the block manager.
  vector<BlockId> all_block_ids;
  RETURN_NOT_OK(fs_manager.block_manager()->GetAllBlockIds(&all_block_ids));

  std::sort(live_block_ids.begin(), live_block_ids.end(), BlockIdCompare());
  std::sort(all_block_ids.begin(), all_block_ids.end(), BlockIdCompare());

  // Blocks found in the block manager but not in a tablet. They are orphaned
  // and can be safely deleted.
  vector<BlockId> orphaned_block_ids;
  std::set_difference(all_block_ids.begin(), all_block_ids.end(),
                      live_block_ids.begin(), live_block_ids.end(),
                      std::back_inserter(orphaned_block_ids), BlockIdCompare());

  // Blocks found in a tablet but not in the block manager. They are missing
  // and indicative of corruption in the associated tablet(s).
  vector<BlockId> missing_block_ids;
  std::set_difference(live_block_ids.begin(), live_block_ids.end(),
                      all_block_ids.begin(), all_block_ids.end(),
                      std::back_inserter(missing_block_ids), BlockIdCompare());

  // Add missing blocks to the report.
  report.missing_block_check.emplace();
  for (const auto& id : missing_block_ids) {
    report.missing_block_check->entries.emplace_back(
        id, FindOrDie(live_block_id_to_tablet, id));
  }

  // Add orphaned blocks to the report after attempting to repair them.
  report.orphaned_block_check.emplace();
  shared_ptr<BlockDeletionTransaction> deletion_transaction;
  if (FLAGS_repair) {
    deletion_transaction = fs_manager.block_manager()->NewDeletionTransaction();
  }
  vector<BlockId> deleted;
  for (const auto& id : orphaned_block_ids) {
    // Opening a block isn't free, but the number of orphaned blocks shouldn't
    // be extraordinarily high.
    uint64_t size;
    {
      unique_ptr<ReadableBlock> block;
      RETURN_NOT_OK(fs_manager.OpenBlock(id, &block));
      RETURN_NOT_OK(block->Size(&size));
    }
    fs::OrphanedBlockCheck::Entry entry(id, size);

    if (FLAGS_repair) {
      deletion_transaction->AddDeletedBlock(id);
    }
    report.orphaned_block_check->entries.emplace_back(entry);
  }

  if (FLAGS_repair) {
    WARN_NOT_OK(deletion_transaction->CommitDeletedBlocks(&deleted),
                "Could not delete orphaned blocks");
    BlockIdSet deleted_set(deleted.begin(), deleted.end());
    for (auto& entry : report.orphaned_block_check->entries) {
      if (ContainsKey(deleted_set, entry.block_id)) entry.repaired = true;
    }
  }

  return report.PrintAndCheckForFatalErrors();
}

Status Format(const RunnerContext& /*context*/) {
  FsManager fs_manager(Env::Default(), FsManagerOpts());
  boost::optional<string> uuid;
  if (!FLAGS_uuid.empty()) {
    uuid = FLAGS_uuid;
  }
  return fs_manager.CreateInitialFileSystemLayout(uuid);
}

Status DumpUuid(const RunnerContext& /*context*/) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.PartialOpen());
  cout << fs_manager.uuid() << endl;
  return Status::OK();
}

Status ParseBlockIdArg(const RunnerContext& context,
                       BlockId* id) {
  const string& block_id_str = FindOrDie(context.required_args, "block_id");
  uint64_t numeric_id;
  if (!safe_strtou64(block_id_str, &numeric_id)) {
    return Status::InvalidArgument(Substitute(
        "Could not parse $0 as numeric block ID", block_id_str));
  }
  *id = BlockId(numeric_id);
  return Status::OK();
}

Status DumpCFile(const RunnerContext& context) {
  BlockId block_id;
  RETURN_NOT_OK(ParseBlockIdArg(context, &block_id));

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  unique_ptr<fs::ReadableBlock> block;
  RETURN_NOT_OK(fs_manager.OpenBlock(block_id, &block));

  unique_ptr<CFileReader> reader;
  RETURN_NOT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));

  if (FLAGS_print_meta) {
    cout << "Header:\n" << pb_util::SecureDebugString(reader->header()) << endl;
    cout << "Footer:\n" << pb_util::SecureDebugString(reader->footer()) << endl;
  }

  if (FLAGS_print_rows) {
    unique_ptr<CFileIterator> it;
    RETURN_NOT_OK(reader->NewIterator(&it, CFileReader::DONT_CACHE_BLOCK, nullptr));
    RETURN_NOT_OK(it->SeekToFirst());

    RETURN_NOT_OK(DumpIterator(*reader, it.get(), &cout, 0, 0));
  }

  return Status::OK();
}

Status DumpBlock(const RunnerContext& context) {
  BlockId block_id;
  RETURN_NOT_OK(ParseBlockIdArg(context, &block_id));

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  unique_ptr<fs::ReadableBlock> block;
  RETURN_NOT_OK(fs_manager.OpenBlock(block_id, &block));

  uint64_t size = 0;
  RETURN_NOT_OK_PREPEND(block->Size(&size), "couldn't get block size");

  faststring buf;
  uint64_t offset = 0;
  while (offset < size) {
    int64_t chunk = std::min<int64_t>(size - offset, 64 * 1024);
    buf.resize(chunk);
    Slice s(buf);
    RETURN_NOT_OK(block->Read(offset, s));
    offset += s.size();
    cout << s.ToString();
  }

  return Status::OK();
}

Status DumpFsTree(const RunnerContext& /*context*/) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  fs_manager.DumpFileSystemTree(std::cout);
  return Status::OK();
}

Status CheckForTabletsThatWillFailWithUpdate() {
  FsManagerOpts opts;
  opts.read_only = true;
  opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs(Env::Default(), std::move(opts));
  RETURN_NOT_OK(fs.Open());

  vector<string> tablet_ids;
  RETURN_NOT_OK(fs.ListTabletIds(&tablet_ids));
  for (const auto& t : tablet_ids) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(TabletMetadata::Load(&fs, t, &meta));
    DataDirGroupPB group;
    Status s = fs.dd_manager()->GetDataDirGroupPB(t, &group);
    if (meta->tablet_data_state() == TabletDataState::TABLET_DATA_TOMBSTONED) {
      // If we just loaded a tombstoned tablet, there should be no in-memory
      // data dir group for the tablet, and the staged directory config won't
      // affect this tablet.
      DCHECK(s.IsNotFound()) << s.ToString();
      continue;
    }
    RETURN_NOT_OK_PREPEND(s, "at least one tablet is configured to use removed data directory. "
        "Retry with --force to override this");
  }
  return Status::OK();
}

Status Update(const RunnerContext& /*context*/) {
  Env* env = Env::Default();

  // First, ensure that if we're removing a data directory, no existing tablets
  // are configured to use it. We approximate this by creating an FsManager
  // that reflects the new data directory configuration, loading all tablet
  // metadata, and retrieving all tablets' data dir groups. If a data dir group
  // cannot be retrieved, it is assumed that it's because the tablet is
  // configured to use a data directory that's now missing.
  //
  // If the user specifies --force, we assume they know what they're doing and
  // skip this check.
  Status s = CheckForTabletsThatWillFailWithUpdate();
  if (FLAGS_force) {
    WARN_NOT_OK(s, "continuing due to --force");
  } else {
    RETURN_NOT_OK_PREPEND(s, "cannot update data directories");
  }

  // Now perform the update.
  FsManagerOpts opts;
  opts.update_instances = UpdateInstanceBehavior::UPDATE_AND_ERROR_ON_FAILURE;
  FsManager fs(env, std::move(opts));
  return fs.Open();
}

// The whole tserver bootstrap procedure is combined by:
// 1. Load LBM containers (FsReport.Stats.lbm_container_count)
// 2. Repair unhealth LBM containers
//    2.1 Remove dead containers (FsReport.Stats.lbm_dead_container_count)
//    2.2 Truncate partial LBM metadata containers (FsReport.partial_record_check)
//    2.3 Remove incompelete containers (FsReport.incomplete_container_check)
//    2.4 Truncate full but have extra space containers (FsReport.full_container_space_check)
//    2.5 Repunch all requested blocks (FsReport.Stats.lbm_need_repunching_block_count)
//    2.6 Rewrite low live block containers (FsReport.Stats.lbm_low_live_block_container_count)
// 3. Load tablet metadata (fs_manager.ListTabletIds())
// 4. Open/Bootstrap tablet (fs_manager.ListTabletIds())
//    4.1 Play Log segments
//    4.2 Start Raft
Status Sample(const RunnerContext& /*context*/) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  FsReport report;
  std::atomic<int> containers_total(0);
  Stopwatch s;
  s.start();
  RETURN_NOT_OK(fs_manager.Open(&report, nullptr, &containers_total));
  s.stop();

  // Stop now if we've already found a fatal error. Otherwise, continue.
  if (report.HasFatalErrors()) {
    RETURN_NOT_OK(report.PrintAndCheckForFatalErrors());
  }

  double estimate_total_bootstrap_sec = 0;
  DataTable table({"type", "count", "latency per item (us)", "total latency (sec)"});
#define ADD_LATENCY(type, sampled_count, latency_per_item_us)                                    \
  do {                                                                                           \
    auto _us = (latency_per_item_us);                                                            \
    int64_t _count = (sampled_count);                                                            \
    double _total_sec = _count * _us / 1e6;                                                      \
    estimate_total_bootstrap_sec += _total_sec;                                                  \
    table.AddRow({type, std::to_string(_count),                                                  \
                  std::to_string(_us), std::to_string(_total_sec)});                             \
  } while (false)

  // 1. Time to load LBM containers.
  double estimate_load_lbm_container_latency_us = report.stats.lbm_container_count == 0 ?
      0 : s.elapsed().wall_micros() / report.stats.lbm_container_count;
  ADD_LATENCY("lbm containers to load",
              containers_total,
              estimate_load_lbm_container_latency_us);

  // 2. Time to repair unhealth LBM containers.
  // 2.1 Remove dead containers (FsReport.Stats.lbm_dead_container_count)
  double sampled_ratio = containers_total == 0 ?
      1 : static_cast<double>(report.stats.lbm_container_count) / containers_total;
  ADD_LATENCY("lbm dead containers to remove",
              report.stats.lbm_dead_container_count / sampled_ratio,
              FLAGS_estimate_latency_ratio_of_remove_file_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us);

  // 2.2 Truncate partial LBM metadata containers (FsReport.partial_record_check)
  ADD_LATENCY("lbm partial metadata to truncate",
              report.partial_record_check->entries.size() / sampled_ratio,
              FLAGS_estimate_latency_ratio_of_truncate_file_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us);

  // 2.3 Remove incompelete containers (FsReport.incomplete_container_check)
  ADD_LATENCY("lbm incomplete containers to remove",
              report.incomplete_container_check->entries.size() / sampled_ratio,
              FLAGS_estimate_latency_ratio_of_remove_file_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us);

  // 2.4 Truncate full but have extra space containers (FsReport.full_container_space_check)
  ADD_LATENCY("lbm full exceed containers to truncate",
              report.full_container_space_check->entries.size() / sampled_ratio,
              FLAGS_estimate_latency_ratio_of_truncate_file_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us);

  // 2.5 Repunch all requested blocks (FsReport.Stats.lbm_need_repunching_block_count)
  ADD_LATENCY("lbm blocks to repunch",
              report.stats.lbm_need_repunching_block_count / sampled_ratio,
              FLAGS_estimate_latency_ratio_of_repunch_block_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us);

  // 2.6 Rewrite low live block containers (FsReport.Stats.lbm_low_live_block_container_count)
  ADD_LATENCY("lbm low live block containers to rewrite",
              report.stats.lbm_low_live_block_container_count / sampled_ratio,
              FLAGS_estimate_latency_ratio_of_rewrite_lbm_metadata_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us /
                      std::max(1UL, FLAGS_fs_max_thread_count_per_data_dir));

  // 3. Time to load tablet metadata.
  vector<string> tablet_ids;
  RETURN_NOT_OK(fs_manager.ListTabletIds(&tablet_ids));
  ADD_LATENCY("tablet metadata to load",
              tablet_ids.size(),
              FLAGS_estimate_latency_ratio_of_load_tablet_metadata_to_load_lbm_container *
                  // --num_tablets_to_open_simultaneously is set to 8 in SensorsData.
                  estimate_load_lbm_container_latency_us / 8);

  // 4. Time to open tablet.
  // 4.1 Play Log segments
  ADD_LATENCY("tablet to bootstrap",
              tablet_ids.size(),
              FLAGS_estimate_latency_ratio_of_bootstrap_tablet_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us);
  // 4.2 Start Raft
  ADD_LATENCY("tablet to start",
              tablet_ids.size(),
              FLAGS_estimate_latency_ratio_of_start_tablet_to_load_lbm_container *
                  estimate_load_lbm_container_latency_us);
#undef ADD_LATENCY

  table.AddRow({"summary", "-", "-",
                std::to_string(static_cast<int64_t>(estimate_total_bootstrap_sec))});

  return table.PrintTo(std::cout);
}

namespace {

// The 'kudu fs list' column fields.
//
// Field is synonymous with a data-table column, but internally we use 'field'
// in order to disambiguate with Kudu columns.
enum class Field {

  // Tablet-specific information:
  kTable,
  kTableId,
  kTabletId,
  kPartition,

  // Rowset-specific information:
  kRowsetId,

  // Block-specific information:
  kBlockId,
  kBlockKind,
  kColumn,
  kColumnId,

  // CFile specific information:
  kCFileDataType,
  kCFileNullable,
  kCFileEncoding,
  kCFileCompression,
  kCFileNumValues,
  kCFileSize,
  kCFileMinKey,
  kCFileMaxKey,
  kCFileIncompatibleFeatures,
  kCFileCompatibleFeatures,
  kCFileDeltaStats,
};

// Enumerable array of field variants. Must be kept in-sync with the Field enum class.
const Field kFieldVariants[] = {
  Field::kTable,
  Field::kTableId,
  Field::kTabletId,
  Field::kPartition,
  Field::kRowsetId,
  Field::kBlockId,
  Field::kBlockKind,
  Field::kColumn,
  Field::kColumnId,
  Field::kCFileDataType,
  Field::kCFileNullable,
  Field::kCFileEncoding,
  Field::kCFileCompression,
  Field::kCFileNumValues,
  Field::kCFileSize,
  Field::kCFileIncompatibleFeatures,
  Field::kCFileCompatibleFeatures,
  Field::kCFileMinKey,
  Field::kCFileMaxKey,
  Field::kCFileDeltaStats,
};

// Groups the fields into categories based on their cardinality and required metadata.
enum class FieldGroup {
  // Cardinality: 1 row per tablet
  // Metadata: TabletMetadata
  kTablet,

  // Cardinality: 1 row per rowset per tablet
  // Metadata: RowSetMetadata, TabletMetadata
  kRowset,

  // Cardinality: 1 row per block per rowset per tablet
  // Metadata: RowSetMetadata, TabletMetadata
  kBlock,

  // Cardinality: 1 row per block per rowset per tablet
  // Metadata: CFileReader, RowSetMetadata, TabletMetadata
  kCFile,
};

// Returns the pretty-printed field name.
const char* ToString(Field field) {
  switch (field) {
    case Field::kTable: return "table";
    case Field::kTableId: return "table-id";
    case Field::kTabletId: return "tablet-id";
    case Field::kPartition: return "partition";
    case Field::kRowsetId: return "rowset-id";
    case Field::kBlockId: return "block-id";
    case Field::kBlockKind: return "block-kind";
    case Field::kColumn: return "column";
    case Field::kColumnId: return "column-id";
    case Field::kCFileDataType: return "cfile-data-type";
    case Field::kCFileNullable: return "cfile-nullable";
    case Field::kCFileEncoding: return "cfile-encoding";
    case Field::kCFileCompression: return "cfile-compression";
    case Field::kCFileNumValues: return "cfile-num-values";
    case Field::kCFileSize: return "cfile-size";
    case Field::kCFileIncompatibleFeatures: return "cfile-incompatible-features";
    case Field::kCFileCompatibleFeatures: return "cfile-compatible-features";
    case Field::kCFileMinKey: return "cfile-min-key";
    case Field::kCFileMaxKey: return "cfile-max-key";
    case Field::kCFileDeltaStats: return "cfile-delta-stats";
  }
  LOG(FATAL) << "unhandled field (this is a bug)";
}

// Returns the pretty-printed group name.
const char* ToString(FieldGroup group) {
  switch (group) {
    case FieldGroup::kTablet: return "tablet";
    case FieldGroup::kRowset: return "rowset";
    case FieldGroup::kBlock: return "block";
    case FieldGroup::kCFile: return "cfile";
    default: LOG(FATAL) << "unhandled field group (this is a bug)";
  }
}

// Parses a field name and returns the corresponding enum variant.
Status ParseField(string name, Field* field) {
  StripWhiteSpace(&name);
  StripString(&name, "_", '-');
  ToLowerCase(&name);

  for (Field variant : kFieldVariants) {
    if (name == ToString(variant)) {
      *field = variant;
      return Status::OK();
    }
  }

  return Status::InvalidArgument("unknown column", name);
}

FieldGroup ToFieldGroup(Field field) {
  switch (field) {
    case Field::kTable:
    case Field::kTableId:
    case Field::kTabletId:
    case Field::kPartition: return FieldGroup::kTablet;

    case Field::kRowsetId: return FieldGroup::kRowset;

    case Field::kBlockId:
    case Field::kBlockKind:
    case Field::kColumn:
    case Field::kColumnId: return FieldGroup::kBlock;

    case Field::kCFileDataType:
    case Field::kCFileNullable:
    case Field::kCFileEncoding:
    case Field::kCFileCompression:
    case Field::kCFileNumValues:
    case Field::kCFileSize:
    case Field::kCFileIncompatibleFeatures:
    case Field::kCFileCompatibleFeatures:
    case Field::kCFileMinKey:
    case Field::kCFileMaxKey:
    case Field::kCFileDeltaStats: return FieldGroup::kCFile;
  }
  LOG(FATAL) << "unhandled field (this is a bug): " << ToString(field);
}

// Returns tablet info for the field.
string TabletInfo(Field field, const TabletMetadata& tablet) {
  switch (field) {
    case Field::kTable: return tablet.table_name();
    case Field::kTableId: return tablet.table_id();
    case Field::kTabletId: return tablet.tablet_id();
    case Field::kPartition: return tablet.partition_schema()
                                         .PartitionDebugString(tablet.partition(),
                                                               *tablet.schema().get());
    default: LOG(FATAL) << "unhandled field (this is a bug): " << ToString(field);
  }
}

// Returns rowset info for the field.
string RowsetInfo(Field field, const TabletMetadata& tablet, const RowSetMetadata& rowset) {
  switch (field) {
    case Field::kRowsetId: return std::to_string(rowset.id());
    default: return TabletInfo(field, tablet);
  }
}

// Returns block info for the field.
string BlockInfo(Field field,
                 const TabletMetadata& tablet,
                 const RowSetMetadata& rowset,
                 const char* block_kind,
                 boost::optional<ColumnId> column_id,
                 const BlockId& block) {
  CHECK(!block.IsNull());
  switch (field) {
    case Field::kBlockId: return std::to_string(block.id());
    case Field::kBlockKind: return block_kind;

    case Field::kColumn: if (column_id) {
      return tablet.schema()->column_by_id(*column_id).name();
    } else { return ""; }

    case Field::kColumnId: if (column_id) {
      return std::to_string(column_id.get());
    } else { return ""; }

    default: return RowsetInfo(field, tablet, rowset);
  }
}

// Formats the min or max primary key property from CFile metadata.
string FormatCFileKeyMetadata(const TabletMetadata& tablet,
                              const CFileReader& cfile,
                              const char* property) {
  string value;
  if (!cfile.GetMetadataEntry(property, &value)) {
    return "";
  }

  Arena arena(1024);
  EncodedKey* key;
  CHECK_OK(EncodedKey::DecodeEncodedString(*tablet.schema().get(), &arena, value, &key));
  return key->Stringify(*tablet.schema().get());
}

// Formats the delta stats property from CFile metadata.
string FormatCFileDeltaStats(const CFileReader& cfile) {
  string value;
  if (!cfile.GetMetadataEntry(tablet::DeltaFileReader::kDeltaStatsEntryName, &value)) {
    return "";
  }

  tablet::DeltaStatsPB deltastats_pb;
  CHECK(deltastats_pb.ParseFromString(value))
      << "failed to decode delta stats for block " << cfile.block_id();

  tablet::DeltaStats deltastats;
  CHECK_OK(deltastats.InitFromPB(deltastats_pb));
  return deltastats.ToString();
}

// Returns cfile info for the field.
string CFileInfo(Field field,
                 const TabletMetadata& tablet,
                 const RowSetMetadata& rowset,
                 const char* block_kind,
                 const boost::optional<ColumnId>& column_id,
                 const BlockId& block,
                 const CFileReader& cfile) {
  switch (field) {
    case Field::kCFileDataType:
      return cfile.type_info()->name();
    case Field::kCFileNullable:
      return cfile.is_nullable() ? "true" : "false";
    case Field::kCFileEncoding:
      return EncodingType_Name(cfile.type_encoding_info()->encoding_type());
    case Field::kCFileCompression:
      return CompressionType_Name(cfile.footer().compression());
    case Field::kCFileNumValues: if (FLAGS_h) {
      return HumanReadableNum::ToString(cfile.footer().num_values());
    } else {
      return std::to_string(cfile.footer().num_values());
    }
    case Field::kCFileSize: if (FLAGS_h) {
      return HumanReadableNumBytes::ToString(cfile.file_size());
    } else {
      return std::to_string(cfile.file_size());
    }
    case Field::kCFileIncompatibleFeatures:
      return std::to_string(cfile.footer().incompatible_features());
    case Field::kCFileCompatibleFeatures:
      return std::to_string(cfile.footer().compatible_features());
    case Field::kCFileMinKey:
      return FormatCFileKeyMetadata(tablet, cfile, tablet::DiskRowSet::kMinKeyMetaEntryName);
    case Field::kCFileMaxKey:
      return FormatCFileKeyMetadata(tablet, cfile, tablet::DiskRowSet::kMaxKeyMetaEntryName);
    case Field::kCFileDeltaStats:
      return FormatCFileDeltaStats(cfile);
    default: return BlockInfo(field, tablet, rowset, block_kind, column_id, block);
  }
}

// Helper function that calls one of the above info functions repeatedly to
// build up a row.
template<typename F, typename... Params>
vector<string> BuildInfoRow(F info_func,
                            const vector<Field>& fields,
                            const Params&... params) {
  vector<string> row;
  row.reserve(fields.size());
  for (Field field : fields) {
    row.emplace_back(info_func(field, params...));
  }
  return row;
}

// Helper function that opens a CFile, if necessary, builds up a row, and adds
// it to the data table.
//
// If the block ID isn't valid or doesn't match the block ID filter, then the
// block is skipped.
Status AddBlockInfoRow(DataTable* table,
                       FieldGroup group,
                       const vector<Field>& fields,
                       FsManager* fs_manager,
                       const TabletMetadata& tablet,
                       const RowSetMetadata& rowset,
                       const char* block_kind,
                       const boost::optional<ColumnId>& column_id,
                       const BlockId& block) {
  if (block.IsNull() || (FLAGS_block_id > 0 && FLAGS_block_id != block.id())) {
    return Status::OK();
  }
  if (group == FieldGroup::kCFile) {
    unique_ptr<CFileReader> cfile;
    unique_ptr<ReadableBlock> readable_block;
    RETURN_NOT_OK(fs_manager->OpenBlock(block, &readable_block));
    RETURN_NOT_OK(CFileReader::Open(std::move(readable_block), ReaderOptions(), &cfile));
    table->AddRow(BuildInfoRow(CFileInfo, fields, tablet, rowset, block_kind,
                               column_id, block, *cfile));

  } else {
    table->AddRow(BuildInfoRow(BlockInfo, fields, tablet, rowset, block_kind,
                               column_id, block));
  }
  return Status::OK();
}
} // anonymous namespace

Status List(const RunnerContext& /*context*/) {
  // Parse the required fields into the enum form, and create an output data table.
  vector<Field> fields;
  vector<string> columns;
  for (StringPiece name : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    Field field = Field::kTable;
    RETURN_NOT_OK(ParseField(name.ToString(), &field));
    fields.push_back(field);
    columns.emplace_back(ToString(field));
  }
  DataTable table(std::move(columns));

  if (fields.empty()) {
    return table.PrintTo(cout);
  }

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.update_instances = UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  // Build the list of tablets to inspect.
  vector<string> tablet_ids;
  if (!FLAGS_tablet_id.empty()) {
    string tablet_id = FLAGS_tablet_id;
    ToLowerCase(&tablet_id);
    tablet_ids.emplace_back(std::move(tablet_id));
  } else {
    RETURN_NOT_OK(fs_manager.ListTabletIds(&tablet_ids));
  }

  string table_name = FLAGS_table_name;
  string table_id = FLAGS_table_id;
  ToLowerCase(&table_id);

  FieldGroup group = ToFieldGroup(*std::max_element(fields.begin(), fields.end()));
  VLOG(1) << "group: " << string(ToString(group));

  for (const string& tablet_id : tablet_ids) {
    scoped_refptr<TabletMetadata> tablet_metadata;
    RETURN_NOT_OK(TabletMetadata::Load(&fs_manager, tablet_id, &tablet_metadata));
    const TabletMetadata& tablet = *tablet_metadata.get();

    if (!table_name.empty() && table_name != tablet.table_name()) {
      continue;
    }

    if (!table_id.empty() && table_id != tablet.table_id()) {
      continue;
    }

    if (group == FieldGroup::kTablet) {
      table.AddRow(BuildInfoRow(TabletInfo, fields, tablet));
      continue;
    }

    for (const auto& rowset_metadata : tablet.rowsets()) {
      const RowSetMetadata& rowset = *rowset_metadata.get();

      if (FLAGS_rowset_id != -1 && FLAGS_rowset_id != rowset.id()) {
        continue;
      }

      if (group == FieldGroup::kRowset) {
        table.AddRow(BuildInfoRow(RowsetInfo, fields, tablet, rowset));
        continue;
      }

      auto column_blocks = rowset.GetColumnBlocksById();
      if (FLAGS_column_id >= 0) {
        ColumnId column_id(FLAGS_column_id);
        auto block = FindOrNull(column_blocks, column_id);
        if (block) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet, rowset,
                                        "column", column_id, *block));
        }
      } else {
        for (const auto& col_block : column_blocks) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                        rowset, "column", col_block.first, col_block.second));
        }
        for (const auto& block : rowset.redo_delta_blocks()) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                        rowset, "redo", boost::none, block));
        }
        for (const auto& block : rowset.undo_delta_blocks()) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                        rowset, "undo", boost::none, block));
        }
        RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                      rowset, "bloom", boost::none, rowset.bloom_block()));
        RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                      rowset, "adhoc-index", boost::none,
                                      rowset.adhoc_index_block()));

      }
    }
    // TODO(dan): should orphaned blocks be included, perhaps behind a flag?
  }
  return table.PrintTo(cout);
}

Status CompactMetaData(const RunnerContext& context) {
  if (FLAGS_metadata_op_type != kMetadataMergeOp) {
    // 判断当前kudu tool的版本，版本必须不高于 1.12.
    const string cmd = "sp_kudu tserver get_flags localhost:7050 "
                       "-flags=log_container_metadata_runtime_compact --format=json";
    vector<string> argv = strings::Split(cmd, " ", strings::SkipEmpty());
    string stderr, stdout;
    RETURN_NOT_OK(Subprocess::Call(argv, "" /* stdin */, &stdout, &stderr));
    JsonReader r(stdout);
    RETURN_NOT_OK(r.Init());
    vector<const Value*> entities;
    RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &entities));
    if (entities.size() >= 1) {
      return Status::RuntimeError("Kudu in this version can compact medatada in runtime.");
    }
  }
  // 通过文件锁的方式，实现多进程安全.
  Env* env = Env::Default();
  const string& filename = "/tmp/kudu_tserver-lbm-metadata-instance.lock";
  FileLock* file_lock;
  RETURN_NOT_OK(env->LockFile(filename, &file_lock));
  KUDU_RETURN_NOT_OK_PREPEND(env->LockFile(filename, &file_lock),
                             "Could not lock instance file. Make sure that "
                             "this tool is not already running.");
  auto unlock = MakeScopedCleanup([&]() {
    env->UnlockFile(file_lock);
  });

  // 只读模式打开fs manager.
  FsManagerOpts fs_opts;
  if (FLAGS_metadata_op_type == kMetadataMergeOp) {
    fs_opts.read_only = false;
  } else {
    fs_opts.read_only = true;
  }
  fs_opts.skip_block_manager = true;
  fs_opts.block_manager_type = "log";
  fs_opts.update_instances = fs::UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(env, std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  // Create a thread pool to compact or merge containers.
  unique_ptr<ThreadPool> pool;
  RETURN_NOT_OK(ThreadPoolBuilder("metadata-compact-pool")
                .set_max_threads(FLAGS_num_threads)
                .Build(&pool));

  atomic<uint64_t> total_blocks = 0;
  atomic<uint64_t> total_live_blocks = 0;
  uint64_t total_containers = 0;
  atomic<bool> seen_fatal_error = false;

  // 遍历所有的container，分别对每个container的metadata进行compact.
  for (const unique_ptr<Dir>& dd : fs_manager.dd_manager()->dirs()) {
    if (seen_fatal_error.load()) {
      break;
    }
    unordered_set<string> container_names;
    Status s = LogBlockManager::GetContainerNames(env, dd.get()->dir(), &container_names);
    if (!s.ok()) {
      WARN_NOT_OK(s, Substitute("Can not fetch container names of disk $0", dd->dir()));
      continue;
    }
    total_containers += container_names.size();
    for (const string& container_name : container_names) {
      if (seen_fatal_error.load()) {
        break;
      }
      RETURN_NOT_OK(pool->Submit([container_name, &total_blocks, &total_live_blocks,
                                  &seen_fatal_error, &dd, &env]() {
        if (seen_fatal_error.load()) {
          return;
        }
        uint64_t live_block_num = 0;
        Status s;
        if (FLAGS_metadata_op_type == kMetadataMergeOp) {
          s = LogBlockManager::MergeLowBlockContainer(env,
                                                      dd.get(),
                                                      container_name);
          LOG(INFO) << "Finish to merge container: " << container_name
                    << ", result: " << s.ToString();
        } else if (FLAGS_metadata_op_type == kMetadataCompactOp) {
          s = LogBlockManager::CompactLowBlockContainer(env, dd.get(),
                                                        container_name,
                                                        &live_block_num);
          LOG(INFO) << "Finish to compact container: " << container_name
                    << ", live block count: " << live_block_num
                    << ", result: " << s.ToString();
        } else {
          DCHECK_EQ(kMetadataPrintOp, FLAGS_metadata_op_type);
          uint64_t total_block_num = 0;
          s = LogBlockManager::PrintLowBlockContainer(env, dd.get(),
                                                      container_name,
                                                      &live_block_num,
                                                      &total_block_num);
          LOG(INFO) << "Finish to print container: " << container_name
                    << ", live block count: " << live_block_num
                    << ", total block count: " << total_block_num
                    << ", result: " << s.ToString();
          total_blocks.fetch_add(total_block_num);
          total_live_blocks.fetch_add(live_block_num);
        }
        if (!s.ok()) {
          WARN_NOT_OK(s, "Container handle failed");
          bool expected_value = false;
          seen_fatal_error.compare_exchange_strong(expected_value, true);
        }
      }));
    }
  }
  pool->Wait();
  if (FLAGS_metadata_op_type == kMetadataPrintOp) {
    LOG(INFO) << "Total containers: " << total_containers
              << ", total blocks: " << total_blocks
              << ", total live blocks: " << total_live_blocks;
  }
  if (seen_fatal_error.load()) {
    LOG(INFO) << "Operation finished, some tasks failed.";
  } else {
    LOG(INFO) << "Operation finished, all tasks succeed.";
  }

  return Status::OK();
}
} // anonymous namespace

static unique_ptr<Mode> BuildFsDumpMode() {
  unique_ptr<Action> dump_cfile =
      ActionBuilder("cfile", &DumpCFile)
      .Description("Dump the contents of a CFile (column file)")
      .ExtraDescription("This interprets the contents of a CFile-formatted block "
                        "and outputs the decoded row data.")
      .AddRequiredParameter({ "block_id", "block identifier" })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("print_meta")
      .AddOptionalParameter("print_rows")
      .Build();

  unique_ptr<Action> dump_block =
      ActionBuilder("block", &DumpBlock)
      .Description("Dump the binary contents of a data block")
      .ExtraDescription("This performs no parsing or interpretation of the data stored "
                        "in the block but rather outputs its binary contents directly.")
      .AddRequiredParameter({ "block_id", "block identifier" })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> dump_tree =
      ActionBuilder("tree", &DumpFsTree)
      .Description("Dump the tree of a Kudu filesystem")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> dump_uuid =
      ActionBuilder("uuid", &DumpUuid)
      .Description("Dump the UUID of a Kudu filesystem")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  return ModeBuilder("dump")
      .Description("Dump a Kudu filesystem")
      .AddAction(std::move(dump_block))
      .AddAction(std::move(dump_cfile))
      .AddAction(std::move(dump_tree))
      .AddAction(std::move(dump_uuid))
      .Build();
}

unique_ptr<Mode> BuildFsMode() {
  unique_ptr<Action> check =
      ActionBuilder("check", &Check)
      .Description("Check a Kudu filesystem for inconsistencies")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("repair")
      .Build();

  unique_ptr<Action> format =
      ActionBuilder("format", &Format)
      .Description("Format a new Kudu filesystem")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("uuid")
      .Build();

  unique_ptr<Action> update =
      ActionBuilder("update_dirs", &Update)
      .Description("Updates the set of data directories in an existing Kudu filesystem")
      .ExtraDescription("If a data directory is in use by a tablet and is "
          "removed, the operation will fail unless --force is also used. "
          "Starting with Kudu 1.12.0, it is not required to run this tool "
          "to add or remove directories. This tool is preserved for backwards "
          "compatibility")
      .AddOptionalParameter("force", boost::none, string("If true, permits "
          "the removal of a data directory that is configured for use by "
          "existing tablets. Those tablets will fail the next time the server "
          "is started"))
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> sample =
      ActionBuilder("sample", &Sample)
          .Description("Sample block containers and tablet metadata in an existing Kudu filesystem")
          .ExtraDescription("Sample block containers and tablet metadata in an existing Kudu "
              "filesystem. The output can be used to estimate the whole view of the Kudu "
              "filesystem.")
          .AddOptionalParameter("estimate_latency_ratio_of_remove_file_to_load_lbm_container")
          .AddOptionalParameter("estimate_latency_ratio_of_truncate_file_to_load_lbm_container")
          .AddOptionalParameter("estimate_latency_ratio_of_repunch_block_to_load_lbm_container")
          .AddOptionalParameter(
              "estimate_latency_ratio_of_rewrite_lbm_metadata_to_load_lbm_container")
          .AddOptionalParameter(
              "estimate_latency_ratio_of_load_tablet_metadata_to_load_lbm_container")
          .AddOptionalParameter("estimate_latency_ratio_of_bootstrap_tablet_to_load_lbm_container")
          .AddOptionalParameter("estimate_latency_ratio_of_start_tablet_to_load_lbm_container")
          .AddOptionalParameter("format")
          .AddOptionalParameter("fs_data_dirs")
          .AddOptionalParameter("fs_metadata_dir")
          .AddOptionalParameter("fs_wal_dir")
          .AddOptionalParameter("log_container_sample_count")
          .Build();

  unique_ptr<Action> list =
      ActionBuilder("list", &List)
      .Description("List metadata for on-disk tablets, rowsets, blocks, and cfiles")
      .ExtraDescription("This tool is useful for discovering and gathering information about "
                        "on-disk data. Many field types can be added to the results with the "
                        "--columns flag, and results can be filtered to a specific table, "
                        "tablet, rowset, column, or block through flags.\n\n"
                        "Note: adding any of the 'cfile' fields to --columns will cause "
                        "the tool to read on-disk metadata for each CFile in the result set, "
                        "which could require large amounts of I/O when many results are returned.")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("table_id")
      .AddOptionalParameter("table_name")
      .AddOptionalParameter("tablet_id")
      .AddOptionalParameter("rowset_id")
      .AddOptionalParameter("column_id")
      .AddOptionalParameter("block_id")
      .AddOptionalParameter("columns", string("tablet-id, rowset-id, block-id, block-kind"),
                            Substitute("Comma-separated list of fields to include in output.\n"
                                       "Possible values: $0",
                                       JoinMapped(kFieldVariants, [] (Field field) {
                                                    return ToString(field);
                                                  }, ", ")))
      .AddOptionalParameter("format")
      .AddOptionalParameter("h")
      .Build();

  unique_ptr<Action> compact_metadata =
      ActionBuilder("compact_metadata", &CompactMetaData)
      .Description("Compact the metadata file of all log block containers")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("metadata_op_type")
      .AddOptionalParameter("num_threads")
      .Build();

  return ModeBuilder("fs")
      .Description("Operate on a local Kudu filesystem")
      .AddMode(BuildFsDumpMode())
      .AddAction(std::move(check))
      .AddAction(std::move(format))
      .AddAction(std::move(list))
      .AddAction(std::move(sample))
      .AddAction(std::move(update))
      .AddAction(std::move(compact_metadata))
      .Build();
}

} // namespace tools
} // namespace kudu
