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

#include "kudu/tablet/tablet_metadata.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/fs/block_id.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

DEFINE_int64(test_row_set_count, 1000, "");
DEFINE_int64(test_block_count_per_rs, 1000, "");

using kudu::log::LogAnchorRegistry;
using kudu::log::MinLogIndexAnchorer;
using std::map;
using std::shared_ptr;
using std::unique_ptr;
using std::unordered_set;

namespace kudu {
namespace tablet {

class TestTabletMetadata : public KuduTabletTest {
 public:
  TestTabletMetadata()
      : KuduTabletTest(GetSimpleTestSchema()) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
    writer_.reset(new LocalTabletWriter(harness_->tablet().get(),
                                        &client_schema_));
  }

  void BuildPartialRow(int key, int intval, const char* strval,
                       unique_ptr<KuduPartialRow>* row);

 protected:
  unique_ptr<LocalTabletWriter> writer_;
};

void TestTabletMetadata::BuildPartialRow(int key, int intval, const char* strval,
                                         unique_ptr<KuduPartialRow>* row) {
  row->reset(new KuduPartialRow(&client_schema_));
  CHECK_OK((*row)->SetInt32(0, key));
  CHECK_OK((*row)->SetInt32(1, intval));
  CHECK_OK((*row)->SetStringCopy(2, strval));
}

// Test that loading & storing the superblock results in an equivalent file.
TEST_F(TestTabletMetadata, TestLoadFromSuperBlock) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  // Write some data to the tablet and flush.
  unique_ptr<KuduPartialRow> row;
  BuildPartialRow(0, 0, "foo", &row);
  ASSERT_OK(writer_->Insert(*row));
  ASSERT_OK(harness_->tablet()->Flush());

  // Create one more rowset. Write and flush.
  BuildPartialRow(1, 1, "bar", &row);
  ASSERT_OK(writer_->Insert(*row));
  ASSERT_OK(harness_->tablet()->Flush());

  // Dump the superblock to a PB. Save the PB to the side.
  TabletSuperBlockPB superblock_pb_0;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb_0));

  // Alter table's extra configuration properties.
  TableExtraConfigPB extra_config;
  extra_config.set_history_max_age_sec(7200);
  NO_FATALS(AlterSchema(*harness_->tablet()->schema(), std::make_optional(extra_config)));

  // Shut down the tablet.
  harness_->tablet()->Shutdown();

  // Dump the superblock to a PB. Save the PB to the side.
  TabletSuperBlockPB superblock_pb_1;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb_1));

  // Check for AlterSchema.
  ASSERT_FALSE(superblock_pb_0.has_extra_config());
  ASSERT_TRUE(superblock_pb_1.has_extra_config());
  ASSERT_TRUE(superblock_pb_1.extra_config().has_history_max_age_sec());
  ASSERT_EQ(7200, superblock_pb_1.extra_config().history_max_age_sec());

  // Test TabletMetadata::ReplaceSuperBlock
  {
    // Load the superblock PB back into the TabletMetadata.
    ASSERT_OK(meta->ReplaceSuperBlock(superblock_pb_1));

    // Dump the tablet metadata to a superblock PB again, and save it.
    TabletSuperBlockPB superblock_pb_2;
    ASSERT_OK(meta->ToSuperBlock(&superblock_pb_2));

    // Compare the 2 dumped superblock PBs.
    ASSERT_EQ(superblock_pb_1.SerializeAsString(),
              superblock_pb_2.SerializeAsString())
                  << pb_util::SecureDebugString(superblock_pb_1)
                  << pb_util::SecureDebugString(superblock_pb_2);
  }

  // Test TabletMetadata::Load
  {
    // Load the TabletMetadata from disk.
    scoped_refptr<TabletMetadata> new_meta;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &new_meta));

    // Dump the tablet metadata to a superblock PB again, and save it.
    TabletSuperBlockPB superblock_pb_2;
    ASSERT_OK(new_meta->ToSuperBlock(&superblock_pb_2));

    // Compare the 2 dumped superblock PBs.
    ASSERT_EQ(superblock_pb_1.SerializeAsString(),
              superblock_pb_2.SerializeAsString())
                  << pb_util::SecureDebugString(superblock_pb_1)
                  << pb_util::SecureDebugString(superblock_pb_2);
  }

  LOG(INFO) << "Superblocks match:\n"
            << pb_util::SecureDebugString(superblock_pb_1);
}

TEST_F(TestTabletMetadata, TestOnDiskSize) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  // The tablet metadata was flushed on creation.
  int64_t initial_size = meta->on_disk_size();
  ASSERT_GT(initial_size, 0);

  // Write some data to the tablet and flush.
  unique_ptr<KuduPartialRow> row;
  BuildPartialRow(0, 0, "foo", &row);
  writer_->Insert(*row);
  ASSERT_OK(harness_->tablet()->Flush());

  // The tablet metadata grows after flushing a new rowset.
  int64_t middle_size = meta->on_disk_size();
  ASSERT_GT(middle_size, initial_size);

  // Create another rowset.
  // The on-disk size shouldn't change until after flush.
  BuildPartialRow(1, 1, "bar", &row);
  writer_->Insert(*row);
  ASSERT_EQ(middle_size, meta->on_disk_size());
  ASSERT_OK(harness_->tablet()->Flush());
  int64_t final_size = meta->on_disk_size();
  ASSERT_GT(final_size, middle_size);

  // Shut down the tablet.
  harness_->tablet()->Shutdown();

  // The tablet metadata is a container file holding the superblock PB,
  // so the on_disk_size should be at least as big.
  TabletSuperBlockPB superblock_pb;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb));
  ASSERT_GE(final_size, superblock_pb.ByteSizeLong());
}

TEST_F(TestTabletMetadata, BenchmarkCollectBlockIds) {
  auto tablet_meta = harness_->tablet()->metadata();
  RowSetMetadataVector rs_metas;
  for (int i = 0; i < FLAGS_test_row_set_count; ++i) {
    unique_ptr<RowSetMetadata> meta;
    ASSERT_OK(RowSetMetadata::CreateNew(tablet_meta, i, &meta));

    map<ColumnId, BlockId> block_by_column;
    for (int j = 0; j < FLAGS_test_block_count_per_rs; ++j) {
      block_by_column[ColumnId(j)] = BlockId(j);
    }
    meta->SetColumnDataBlocks(block_by_column);
    rs_metas.emplace_back(shared_ptr<RowSetMetadata>(meta.release()));
  }
  tablet_meta->UpdateAndFlush(RowSetMetadataIds(), rs_metas, TabletMetadata::kNoMrsFlushed);

  for (int i = 0; i < 10; ++i) {
    BlockIdContainer block_ids;
    LOG_TIMING(INFO, "collecting BlockIds") {
      block_ids = tablet_meta->CollectBlockIds();
    }
    LOG(INFO) << "block_ids size: " << block_ids.size();
  }
}

TEST_F(TestTabletMetadata, TestTxnMetadata) {
  constexpr const char* kOwner = "txn";
  const Timestamp kDummyTimestamp = Timestamp(1337);
  scoped_refptr<LogAnchorRegistry> registry(new LogAnchorRegistry);
  TabletMetadata* meta = harness_->tablet()->metadata();
  const auto make_anchor = [&] () {
    return unique_ptr<MinLogIndexAnchorer>(new MinLogIndexAnchorer(registry.get(), kOwner));
  };
  int64_t kCommittedTxnId = 1;
  int64_t kAbortedTxnId = 2;
  int64_t kInFlightTxnId = 3;
  meta->AddTxnMetadata(kCommittedTxnId, make_anchor());
  meta->BeginCommitTransaction(kCommittedTxnId, kDummyTimestamp, make_anchor());
  meta->AddCommitTimestamp(kCommittedTxnId, kDummyTimestamp, make_anchor());
  ASSERT_EQ(1, meta->GetTxnMetadata().size());

  meta->AddTxnMetadata(kAbortedTxnId, make_anchor());
  meta->BeginCommitTransaction(kAbortedTxnId, kDummyTimestamp, make_anchor());
  meta->AbortTransaction(kAbortedTxnId, make_anchor());
  ASSERT_EQ(2, meta->GetTxnMetadata().size());

  meta->AddTxnMetadata(kInFlightTxnId, make_anchor());
  meta->BeginCommitTransaction(kInFlightTxnId, kDummyTimestamp, make_anchor());
  ASSERT_EQ(3, meta->GetTxnMetadata().size());

  // Validate the transactions' fields.
  const auto validate_txn_metas = [&] (TabletMetadata* meta) {
    auto txn_metas = meta->GetTxnMetadata();
    ASSERT_EQ(3, txn_metas.size());
    ASSERT_TRUE(meta->HasTxnMetadata(kCommittedTxnId));
    ASSERT_TRUE(meta->HasTxnMetadata(kAbortedTxnId));
    ASSERT_TRUE(meta->HasTxnMetadata(kInFlightTxnId));

    const auto& committed_txn = FindOrDie(txn_metas, kCommittedTxnId);
    ASSERT_FALSE(committed_txn->aborted());
    ASSERT_TRUE(committed_txn->commit_mvcc_op_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *committed_txn->commit_mvcc_op_timestamp());
    ASSERT_TRUE(committed_txn->commit_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *committed_txn->commit_timestamp());

    const auto& aborted_txn = FindOrDie(txn_metas, kAbortedTxnId);
    ASSERT_TRUE(aborted_txn->aborted());
    ASSERT_FALSE(aborted_txn->commit_timestamp().has_value());
    ASSERT_TRUE(aborted_txn->commit_mvcc_op_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *aborted_txn->commit_mvcc_op_timestamp());

    const auto& in_flight_txn = FindOrDie(txn_metas, kInFlightTxnId);
    ASSERT_FALSE(in_flight_txn->aborted());
    ASSERT_FALSE(in_flight_txn->commit_timestamp().has_value());
    ASSERT_TRUE(in_flight_txn->commit_mvcc_op_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *in_flight_txn->commit_mvcc_op_timestamp());

    unordered_set<int64_t> in_flight_txn_ids;
    unordered_set<int64_t> terminal_txn_ids;
    meta->GetTxnIds(&in_flight_txn_ids, &terminal_txn_ids);
    ASSERT_EQ(1, in_flight_txn_ids.size());
    ASSERT_EQ(2, terminal_txn_ids.size());
    ASSERT_TRUE(ContainsKey(in_flight_txn_ids, kInFlightTxnId));
    ASSERT_TRUE(ContainsKey(terminal_txn_ids, kAbortedTxnId));
    ASSERT_TRUE(ContainsKey(terminal_txn_ids, kCommittedTxnId));
  };
  NO_FATALS(validate_txn_metas(meta));

  // Validate the superblock fields are set.
  TabletSuperBlockPB superblock_pb;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb));
  ASSERT_EQ(3, superblock_pb.txn_metadata_size());

  // Flush and reload the metadata.
  ASSERT_OK(meta->Flush());
  scoped_refptr<TabletMetadata> new_meta;
  ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                  harness_->tablet()->tablet_id(),
                                  &new_meta));
  NO_FATALS(validate_txn_metas(new_meta.get()));
}

} // namespace tablet
} // namespace kudu
