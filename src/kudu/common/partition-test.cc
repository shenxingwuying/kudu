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

#include "kudu/common/partition.h"

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::optional;
using google::protobuf::util::MessageDifferencer;
using std::pair;
using std::string;
using std::vector;
using std::make_pair;
using strings::Substitute;

namespace kudu {

namespace {
void AddHashDimension(PartitionSchemaPB* partition_schema_pb,
                      const vector<string>& columns,
                      int32_t num_buckets,
                      uint32_t seed) {
  auto* hash_dimension = partition_schema_pb->add_hash_schema();
  for (const string& column : columns) {
    hash_dimension->add_columns()->set_name(column);
  }
  hash_dimension->set_num_buckets(num_buckets);
  hash_dimension->set_seed(seed);
}

void SetRangePartitionComponent(PartitionSchemaPB* partition_schema_pb,
                                const vector<string>& columns) {
  PartitionSchemaPB::RangeSchemaPB* range_schema = partition_schema_pb->mutable_range_schema();
  range_schema->Clear();
  for (const string& column : columns) {
    range_schema->add_columns()->set_name(column);
  }
}

void CheckCreateRangePartitions(const vector<pair<optional<string>, optional<string>>>& raw_bounds,
                                const vector<string>& raw_splits,
                                const vector<pair<string, string>>& expected_partition_ranges) {
  CHECK_EQ(std::max(raw_bounds.size(), 1UL) + raw_splits.size(), expected_partition_ranges.size());

  // CREATE TABLE t (col STRING PRIMARY KEY),
  // PARITITION BY RANGE (col);
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("col", STRING) }, { ColumnId(0) }, 1));
  Schema& schema = *schema_ptr.get();
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(PartitionSchemaPB(), schema, &partition_schema));

  ASSERT_EQ("RANGE (col)", partition_schema.DebugString(schema));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds;
  for (const auto& bound : raw_bounds) {
    const optional<string>& lower = bound.first;
    const optional<string>& upper = bound.second;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    if (lower) {
      ASSERT_OK(lower_bound.SetStringCopy("col", *lower));
    }
    if (upper) {
      ASSERT_OK(upper_bound.SetStringCopy("col", *upper));
    }
    bounds.emplace_back(std::move(lower_bound), std::move(upper_bound));
  }

  vector<KuduPartialRow> splits;
  for (const string& split_value : raw_splits) {
    KuduPartialRow split(&schema);
    ASSERT_OK(split.SetStringCopy("col", split_value));
    splits.emplace_back(split);
  }

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(splits, bounds, {}, schema_ptr, &partitions));
  ASSERT_EQ(expected_partition_ranges.size(), partitions.size());

  for (int i = 0; i < partitions.size(); i++) {
    const string& lower = expected_partition_ranges[i].first;
    const string& upper = expected_partition_ranges[i].second;
    SCOPED_TRACE(Substitute("partition $0", i));

    const auto& p = partitions[i];
    ASSERT_TRUE(p.hash_buckets().empty());
    ASSERT_TRUE(p.end().hash_key().empty());
    ASSERT_TRUE(p.begin().hash_key().empty());
    ASSERT_EQ(lower, p.begin().range_key());
    ASSERT_EQ(upper, p.end().range_key());
    ASSERT_EQ(lower, p.begin().ToString());
    ASSERT_EQ(upper, p.end().ToString());
  }
}

} // namespace

class PartitionTest : public KuduTest {};

// Tests that missing values are correctly filled in with minimums when creating
// range partition keys, and that completely missing keys are encoded as the
// logical minimum and logical maximum for lower and upper bounds, respectively.
TEST_F(PartitionTest, TestCompoundRangeKeyEncoding) {

  // CREATE TABLE t (c1 STRING, c2 STRING, c3 STRING),
  // PRIMARY KEY (c1, c2, c3)
  // PARTITION BY RANGE (c1, c2, c3);
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("c1", STRING),
                  ColumnSchema("c2", STRING),
                  ColumnSchema("c3", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("RANGE (c1, c2, c3)", partition_schema.DebugString(schema));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds;
  vector<KuduPartialRow> splits;

  { // [(_, _, _), (_, _, b))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(upper.SetStringCopy("c3", "b"));
    bounds.emplace_back(std::move(lower), std::move(upper));
  }

  { // [(_, b, c), (d, _, f))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("c2", "b"));
    ASSERT_OK(lower.SetStringCopy("c3", "c"));
    ASSERT_OK(upper.SetStringCopy("c1", "d"));
    ASSERT_OK(upper.SetStringCopy("c3", "f"));
    bounds.emplace_back(std::move(lower), std::move(upper));
  }

  { // [(e, _, _), (_, _, _))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("c1", "e"));
    bounds.emplace_back(std::move(lower), std::move(upper));
  }

  { // split: (_, _, a)
    KuduPartialRow split(&schema);
    ASSERT_OK(split.SetStringCopy("c3", "a"));
    splits.emplace_back(std::move(split));
  }

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(splits, bounds, {}, schema_ptr, &partitions));
  ASSERT_EQ(4, partitions.size());

  EXPECT_TRUE(partitions[0].hash_buckets().empty());

  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION VALUES < ("", "", "a"))",
            partition_schema.PartitionDebugString(partitions[0], schema));
  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION ("", "", "a") <= VALUES < ("", "", "b"))",
            partition_schema.PartitionDebugString(partitions[1], schema));
  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION ("", "b", "c") <= VALUES < ("d", "", "f"))",
            partition_schema.PartitionDebugString(partitions[2], schema));
  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION VALUES >= ("e", "", ""))",
            partition_schema.PartitionDebugString(partitions[3], schema));
}

TEST_F(PartitionTest, PartitionKeyEncoding) {
  // CREATE TABLE t (a INT32, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARTITION BY [HASH BUCKET (a, b), HASH BUCKET (c), RANGE (a, b, c)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", INT32),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  AddHashDimension(&schema_builder, { "a", "b" }, 32, 0);
  AddHashDimension(&schema_builder, { "c" }, 32, 42);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("HASH (a, b) PARTITIONS 32, HASH (c) PARTITIONS 32 SEED 42, RANGE (a, b, c)",
            partition_schema.DebugString(schema));

  {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 0));
    const auto key = partition_schema.EncodeKey(row);
    EXPECT_EQ(string("\0\0\0\0"     // hash(0, "")
                     "\0\0\0\x14",  // hash("")
                     8), key.hash_key());
    EXPECT_EQ(string("\x80\0\0\0"   // a = 0
                     "\0\0",        // b = ""; c is elided
                     6), key.range_key());
    EXPECT_EQ(string("\0\0\0\0"
                     "\0\0\0\x14"
                     "\x80\0\0\0"
                     "\0\0",
                     14), key.ToString());

    const string expected =
        R"(HASH (a, b): 0, HASH (c): 20, RANGE (a, b, c): (0, "", ""))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 1));
    const auto key = partition_schema.EncodeKey(row);

    EXPECT_EQ(string("\0\0\0\x5"    // hash(1, "")
                     "\0\0\0\x14",  // hash("")
                     8), key.hash_key());
    EXPECT_EQ(string("\x80\0\0\x01" // a = 1
                     "\0\0",        // b = ""; c is elided
                     6), key.range_key());
    EXPECT_EQ(string("\0\0\0\x5"
                     "\0\0\0\x14"
                     "\x80\0\0\x01"
                     "\0\0",
                     14), key.ToString());

    const string expected =
        R"(HASH (a, b): 5, HASH (c): 20, RANGE (a, b, c): (1, "", ""))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 0));
    ASSERT_OK(row.SetStringCopy("b", "b"));
    ASSERT_OK(row.SetStringCopy("c", "c"));
    const auto key = partition_schema.EncodeKey(row);

    EXPECT_EQ(string("\0\0\0\x1A" // hash(0, "b")
                     "\0\0\0\x1D",// hash("c")
                     8), key.hash_key());
    EXPECT_EQ(string("\x80\0\0\0" // a = 0
                     "b\0\0"      // b = "b"
                     "c",         // c = "c"
                     8), key.range_key());
    EXPECT_EQ(string("\0\0\0\x1A"
                     "\0\0\0\x1D"
                     "\x80\0\0\0"
                     "b\0\0"
                     "c",
                     16), key.ToString());

    const string expected =
        R"(HASH (a, b): 26, HASH (c): 29, RANGE (a, b, c): (0, "b", "c"))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 1));
    ASSERT_OK(row.SetStringCopy("b", "b"));
    ASSERT_OK(row.SetStringCopy("c", "c"));
    const auto key = partition_schema.EncodeKey(row);

    EXPECT_EQ(string("\0\0\0\x0"   // hash(1, "b")
                     "\0\0\0\x1D", // hash("c")
                     8), key.hash_key());
    EXPECT_EQ(string("\x80\0\0\x1" // a = 1
                     "b\0\0"       // b = "b"
                     "c",          // c = "c"
                     8), key.range_key());
    EXPECT_EQ(string("\0\0\0\x0"
                     "\0\0\0\x1D"
                     "\x80\0\0\x1"
                     "b\0\0"
                     "c",
                     16), key.ToString());

    const string expected =
        R"(HASH (a, b): 0, HASH (c): 29, RANGE (a, b, c): (1, "b", "c"))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    // Check that row values are redacted when the log redact flag is set.
    ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 1));
    ASSERT_OK(row.SetStringCopy("b", "b"));
    ASSERT_OK(row.SetStringCopy("c", "c"));
    const auto key = partition_schema.EncodeKey(row);

    const string expected =
        R"(HASH (a, b): 0, HASH (c): 29, RANGE (a, b, c): (<redacted>, <redacted>, <redacted>))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));

    // Check that row values are redacted from error messages when
    // --redact is set with 'log'.

    EXPECT_EQ("<hash-decode-error>",
              partition_schema.PartitionKeyDebugString(
                  PartitionKey(string("\0\1\0\1", 4), ""), schema));
    EXPECT_EQ("HASH (a, b): 0, HASH (c): 0, RANGE (a, b, c): "
              "<range-key-decode-error: Invalid argument: "
              "Error decoding partition key range component 'a': key too short: <redacted>>",
              partition_schema.PartitionKeyDebugString(
                  PartitionKey(string("\0\0\0\0\0\0\0\0", 8), "a"), schema));
  }
}

TEST_F(PartitionTest, TestCreateRangePartitions) {
  {
    // Splits:
    // { a: "1" }
    // { a: "2" }
    // { a: "2\0" }
    //
    // Encoded Partition Keys:
    // [ ( ""), ("1") )
    // [ ("1"), ("2") )
    // [ ("2"), ("2\0") )
    // [ ("2\0"), ("") )

    vector<string> splits { "2", "1", string("2\0", 2) };
    vector<pair<string, string>> partitions {
      { "", "1" },
      { "1", "2" },
      { "2", string("2\0", 2) },
      { string("2\0", 2), "" },
    };
    NO_FATALS(CheckCreateRangePartitions({}, splits, partitions));
  }
  {
    // Bounds
    //
    // ({ a: "a" }, { a: "m" })
    // ({ a: "x" }, { a: "z" })
    //
    // Encoded Partition Keys:
    //
    // [ ("a"), ("m") )
    // [ ("x"), ("z") )

    vector<pair<optional<string>, optional<string>>> bounds {
      { string("a"), string("m") },
      { string("x"), string("z") },
    };
    vector<pair<string, string>> partitions {
      { "a", "m" },
      { "x", "z" },
    };
    NO_FATALS(CheckCreateRangePartitions(bounds, {}, partitions));
  }
  {
    // Bounds:
    // ({          }, { col: "b" })
    // ({ col: "c" }, { col: "f" })
    // ({ col: "f" }, { col: "z" })
    //
    // Splits:
    // { col: "a"   }
    // { col: "d"   }
    // { col: "e"   }
    // { col: "h"   }
    // { col: "h\0" }
    //
    // Encoded Partition Keys:
    // [ (""),    ("a")   )
    // [ ("a"),   ("b")   )
    // [ ("c"),   ("d")   )
    // [ ("d"),   ("e")   )
    // [ ("e"),   ("f")   )
    // [ ("f"),   ("h")   )
    // [ ("h"),   ("h\0") )
    // [ ("h\0"), ("z")   )

    vector<pair<optional<string>, optional<string>>> bounds {
      { boost::none, string("b") },
      { string("c"), string("f") },
      { string("f"), string("z") },
    };
    vector<string> splits { "d", "a", "h", "e", string("h\0", 2) };
    vector<pair<string, string>> partitions {
      { "", "a" },
      { "a", "b" },
      { "c", "d" },
      { "d", "e" },
      { "e", "f" },
      { "f", "h" },
      { "h", string("h\0", 2) },
      { string("h\0", 2), "z" },
    };
    NO_FATALS(CheckCreateRangePartitions(bounds, splits, partitions));
  }
  {
    // Bounds:
    // ({ }, { })
    //
    // Splits:
    // { col: "m"   }
    //
    // Encoded Partition Keys:
    // [ (""),    ("m")   )
    // [ ("m"),   ("")   )

    vector<pair<optional<string>, optional<string>>> bounds {
      { boost::none, boost::none },
    };
    vector<string> splits { "m" };
    vector<pair<string, string>> partitions {
      { "", "m" },
      { "m", "" },
    };
    NO_FATALS(CheckCreateRangePartitions(bounds, splits, partitions));
  }
}

TEST_F(PartitionTest, TestCreateHashPartitions) {
  // CREATE TABLE t (a VARCHAR PRIMARY KEY),
  // PARTITION BY [HASH BUCKET (a)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING) }, { ColumnId(0) }, 1));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  SetRangePartitionComponent(&schema_builder, vector<string>());
  AddHashDimension(&schema_builder, { "a" }, 3, 42);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("HASH (a) PARTITIONS 3 SEED 42", partition_schema.DebugString(schema));

  // Encoded Partition Keys:
  //
  // [ (_), (1) )
  // [ (1), (2) )
  // [ (2), (_) )

  vector<Partition> partitions;
  ASSERT_OK(
      partition_schema.CreatePartitions(vector<KuduPartialRow>(), {}, {}, schema_ptr, &partitions));
  ASSERT_EQ(3, partitions.size());

  EXPECT_EQ(0, partitions[0].hash_buckets()[0]);
  EXPECT_TRUE(partitions[0].begin().range_key().empty());
  EXPECT_TRUE(partitions[0].end().range_key().empty());
  EXPECT_TRUE(partitions[0].begin().empty());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[0].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 0",
            partition_schema.PartitionDebugString(partitions[0], schema));

  EXPECT_EQ(1, partitions[1].hash_buckets()[0]);
  EXPECT_TRUE(partitions[1].begin().range_key().empty());
  EXPECT_TRUE(partitions[1].end().range_key().empty());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[1].begin().ToString());
  EXPECT_EQ(string("\0\0\0\2", 4), partitions[1].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 1",
            partition_schema.PartitionDebugString(partitions[1], schema));

  EXPECT_EQ(2, partitions[2].hash_buckets()[0]);
  EXPECT_TRUE(partitions[2].begin().range_key().empty());
  EXPECT_TRUE(partitions[2].end().range_key().empty());
  EXPECT_EQ(string("\0\0\0\2", 4), partitions[2].begin().ToString());
  EXPECT_EQ("", partitions[2].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 2",
            partition_schema.PartitionDebugString(partitions[2], schema));
}

TEST_F(PartitionTest, TestCreatePartitions) {
  // Explicitly enable redaction. It should have no effect on the subsequent
  // partition pretty printing tests, as partitions are metadata and thus not
  // redacted.
  ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));

  // CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARTITION BY [HASH BUCKET (a), HASH BUCKET (b), RANGE (a, b, c)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  AddHashDimension(&schema_builder, { "a" }, 2, 0);
  AddHashDimension(&schema_builder, { "b" }, 2, 0);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("HASH (a) PARTITIONS 2, HASH (b) PARTITIONS 2, RANGE (a, b, c)",
            partition_schema.DebugString(schema));

  // Split Rows:
  //
  // { a: "a1", b: "b1", c: "c1" }
  // { b: "a2", b: "b2" }
  //
  // non-specified column values default to the logical minimum value ("").
  //
  // Encoded Partition Keys:
  //
  // [ (_, _,        _), (0, 0, "a1b1c1") )
  // [ (0, 0, "a1b1c1"), (0, 0,   "a2b2") )
  // [ (0, 0,   "a2b2"), (0, 1,        _) )
  //
  // [ (0, 1,        _), (0, 1, "a1b1c1") )
  // [ (0, 1, "a1b1c1"), (0, 1,   "a2b2") )
  // [ (0, 1,   "a2b2"), (1, _,        _) )
  //
  // [ (1, _,        _), (1, 0, "a1b1c1") )
  // [ (1, 0, "a1b1c1"), (1, 0,   "a2b2") )
  // [ (1, 0,   "a2b2"), (1, 1,        _) )
  //
  // [ (1, 1,        _), (1, 1, "a1b1c1") )
  // [ (1, 1, "a1b1c1"), (1, 1,   "a2b2") )
  // [ (1, 1,   "a2b2"), (_, _,        _) )
  //
  // _ signifies that the value is omitted from the encoded partition key.

  KuduPartialRow split_a(&schema);
  ASSERT_OK(split_a.SetStringCopy("a", "a1"));
  ASSERT_OK(split_a.SetStringCopy("b", "b1"));
  ASSERT_OK(split_a.SetStringCopy("c", "c1"));

  KuduPartialRow split_b(&schema);
  ASSERT_OK(split_b.SetStringCopy("a", "a2"));
  ASSERT_OK(split_b.SetStringCopy("b", "b2"));

  // Split keys need not be passed in sorted order.
  vector<KuduPartialRow> split_rows = { split_b, split_a };
  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(split_rows, {}, {}, schema_ptr, &partitions));
  ASSERT_EQ(12, partitions.size());

  EXPECT_EQ(0, partitions[0].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[0].hash_buckets()[1]);
  EXPECT_EQ("", partitions[0].begin().range_key());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[0].end().range_key());
  EXPECT_EQ("", partitions[0].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a1\0\0b1\0\0c1", 18),
            partitions[0].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[0], schema));


  EXPECT_EQ(0, partitions[1].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[1].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[1].begin().range_key());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[1].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a1\0\0b1\0\0c1", 18),
            partitions[1].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a2\0\0b2\0\0", 16),
            partitions[1].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[1], schema));

  EXPECT_EQ(0, partitions[2].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[2].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[2].begin().range_key());
  EXPECT_EQ("", partitions[2].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a2\0\0b2\0\0", 16), partitions[2].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1", 8), partitions[2].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[2], schema));

  EXPECT_EQ(0, partitions[3].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[3].hash_buckets()[1]);
  EXPECT_EQ("", partitions[3].begin().range_key());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[3].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1", 8), partitions[3].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a1\0\0b1\0\0c1", 18), partitions[3].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[3], schema));

  EXPECT_EQ(0, partitions[4].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[4].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[4].begin().range_key());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[4].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a1\0\0b1\0\0c1", 18),
            partitions[4].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[4].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[4], schema));

  EXPECT_EQ(0, partitions[5].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[5].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[5].begin().range_key());
  EXPECT_EQ("", partitions[5].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[5].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[5].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[5], schema));

  EXPECT_EQ(1, partitions[6].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[6].hash_buckets()[1]);
  EXPECT_EQ("", partitions[6].begin().range_key());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[6].end().range_key());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[6].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a1\0\0b1\0\0c1", 18), partitions[6].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[6], schema));

  EXPECT_EQ(1, partitions[7].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[7].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[7].begin().range_key());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[7].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a1\0\0b1\0\0c1", 18),
            partitions[7].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a2\0\0b2\0\0", 16), partitions[7].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[7], schema));

  EXPECT_EQ(1, partitions[8].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[8].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[8].begin().range_key());
  EXPECT_EQ("", partitions[8].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a2\0\0b2\0\0", 16), partitions[8].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1", 8), partitions[8].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[8], schema));

  EXPECT_EQ(1, partitions[9].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[9].hash_buckets()[1]);
  EXPECT_EQ("", partitions[9].begin().range_key());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[9].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1", 8), partitions[9].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a1\0\0b1\0\0c1", 18), partitions[9].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[9], schema));

  EXPECT_EQ(1, partitions[10].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[10].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[10].begin().range_key());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[10].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a1\0\0b1\0\0c1", 18),
            partitions[10].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[10].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[10], schema));

  EXPECT_EQ(1, partitions[11].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[11].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[11].begin().range_key());
  EXPECT_EQ("", partitions[11].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[11].begin().ToString());
  EXPECT_EQ("", partitions[11].end().ToString());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[11], schema));
}

TEST_F(PartitionTest, TestIncrementRangePartitionBounds) {
  // CREATE TABLE t (a INT8, b INT8, c INT8, PRIMARY KEY (a, b, c))
  // PARTITION BY RANGE (a, b, c);
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("c1", INT8),
                  ColumnSchema("c2", INT8),
                  ColumnSchema("c3", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  vector<vector<boost::optional<int8_t>>> tests {
    // Big list of test cases. First three columns are the input columns, final
    // three columns are the expected output columns. For example,
    { 1, 2, 3, 1, 2, 4 },
    // corresponds to the test case:
    // (1, 2, 3) -> (1, 2, 4)

    { 1, 2, boost::none, 1, 2, -127 },
    { 1, boost::none, 3, 1, boost::none, 4 },
    { boost::none, 2, 3, boost::none, 2, 4 },
    { 1, boost::none, boost::none, 1, boost::none, -127 },
    { boost::none, boost::none, 3, boost::none, boost::none, 4 },
    { boost::none, 2, boost::none, boost::none, 2, -127 },
    { 1, 2, 127, 1, 3, boost::none },
    { 1, 127, 3, 1, 127, 4},
    { 1, 127, 127, 2, boost::none, boost::none },
  };

  auto check = [&] (const vector<boost::optional<int8_t>>& test, bool lower_bound) {
    CHECK_EQ(6, test.size());
    KuduPartialRow bound(&schema);
    if (test[0]) ASSERT_OK(bound.SetInt8("c1", *test[0]));
    if (test[1]) ASSERT_OK(bound.SetInt8("c2", *test[1]));
    if (test[2]) ASSERT_OK(bound.SetInt8("c3", *test[2]));

    vector<string> components;
    partition_schema.AppendRangeDebugStringComponentsOrMin(bound, &components);
    SCOPED_TRACE(JoinStrings(components, ", "));
    SCOPED_TRACE(lower_bound ? "lower bound" : "upper bound");

    if (lower_bound) {
      ASSERT_OK(partition_schema.MakeLowerBoundRangePartitionKeyInclusive(&bound));
    } else {
      ASSERT_OK(partition_schema.MakeUpperBoundRangePartitionKeyExclusive(&bound));
    }

    int8_t val;
    if (test[3]) {
      ASSERT_OK(bound.GetInt8("c1", &val));
      ASSERT_EQ(*test[3], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c1"));
    }
    if (test[4]) {
      ASSERT_OK(bound.GetInt8("c2", &val));
      ASSERT_EQ(*test[4], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c2"));
    }
    if (test[5]) {
      ASSERT_OK(bound.GetInt8("c3", &val));
      ASSERT_EQ(*test[5], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c3"));
    }
  };

  for (const auto& test : tests) {
    check(test, true);
    check(test, false);
  }

  // Special cases:
  // lower bound: (_, _, _) -> (_, _, -127)
  check({ boost::none, boost::none, boost::none, boost::none, boost::none, -127 }, true);
  // upper bound: (_, _, _) -> (_, _, _)
  check({ boost::none, boost::none, boost::none, boost::none, boost::none, boost::none }, false);
  // upper bound: (127, 127, 127) -> (_, _, _)
  check({ 127, 127, 127, boost::none, boost::none, boost::none }, false);

  // lower bound: (127, 127, 127) -> fail!
    KuduPartialRow lower_bound(&schema);
    ASSERT_OK(lower_bound.SetInt8("c1", 127));
    ASSERT_OK(lower_bound.SetInt8("c2", 127));
    ASSERT_OK(lower_bound.SetInt8("c3", 127));
    Status s = partition_schema.MakeLowerBoundRangePartitionKeyInclusive(&lower_bound);
    ASSERT_EQ("Invalid argument: Exclusive lower bound range partition key must not have "
              "maximum values for all components: (127, 127, 127)",
              s.ToString());
}

TEST_F(PartitionTest, TestIncrementRangePartitionStringBounds) {
  // CREATE TABLE t (a STRING, b STRING, PRIMARY KEY (a, b))
  // PARTITION BY RANGE (a, b, c);
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("c1", STRING),
                  ColumnSchema("c2", STRING) },
                { ColumnId(0), ColumnId(1) }, 2));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  vector<vector<boost::optional<string>>> tests {
    { string("a"), string("b"), string("a"), string("b\0", 2) },
    { string("a"), boost::none, string("a"), string("\0", 1) },
  };

  auto check = [&] (const vector<boost::optional<string>>& test, bool lower_bound) {
    CHECK_EQ(4, test.size());
    KuduPartialRow bound(&schema);
    if (test[0]) ASSERT_OK(bound.SetString("c1", *test[0]));
    if (test[1]) ASSERT_OK(bound.SetString("c2", *test[1]));

    vector<string> components;
    partition_schema.AppendRangeDebugStringComponentsOrMin(bound, &components);
    SCOPED_TRACE(JoinStrings(components, ", "));
    SCOPED_TRACE(lower_bound ? "lower bound" : "upper bound");

    if (lower_bound) {
      ASSERT_OK(partition_schema.MakeLowerBoundRangePartitionKeyInclusive(&bound));
    } else {
      ASSERT_OK(partition_schema.MakeUpperBoundRangePartitionKeyExclusive(&bound));
    }

    Slice val;
    if (test[2]) {
      ASSERT_OK(bound.GetString("c1", &val));
      ASSERT_EQ(*test[2], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c1"));
    }
    if (test[3]) {
      ASSERT_OK(bound.GetString("c2", &val));
      ASSERT_EQ(*test[3], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c2"));
    }
  };

  for (const auto& test : tests) {
    check(test, true);
    check(test, false);
  }
}

TEST_F(PartitionTest, TestVarcharRangePartitions) {
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("c1", VARCHAR, false, nullptr, nullptr,
                               ColumnStorageAttributes(), ColumnTypeAttributes(10)),
                  ColumnSchema("c2", VARCHAR, false, nullptr, nullptr,
                               ColumnStorageAttributes(), ColumnTypeAttributes(10)) },
                  { ColumnId(0), ColumnId(1) }, 2));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  vector<vector<boost::optional<string>>> tests {
    { string("a"), string("b"), string("a"), string("b\0", 2) },
    { string("a"), boost::none, string("a"), string("\0", 1) },
  };

  auto check = [&] (const vector<boost::optional<string>>& test, bool lower_bound) {
    CHECK_EQ(4, test.size());
    KuduPartialRow bound(&schema);
    if (test[0]) ASSERT_OK(bound.SetVarchar("c1", *test[0]));
    if (test[1]) ASSERT_OK(bound.SetVarchar("c2", *test[1]));

    vector<string> components;
    partition_schema.AppendRangeDebugStringComponentsOrMin(bound, &components);
    SCOPED_TRACE(JoinStrings(components, ", "));
    SCOPED_TRACE(lower_bound ? "lower bound" : "upper bound");

    if (lower_bound) {
      ASSERT_OK(partition_schema.MakeLowerBoundRangePartitionKeyInclusive(&bound));
    } else {
      ASSERT_OK(partition_schema.MakeUpperBoundRangePartitionKeyExclusive(&bound));
    }

    Slice val;
    if (test[2]) {
      ASSERT_OK(bound.GetVarchar("c1", &val));
      ASSERT_EQ(*test[2], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c1"));
    }
    if (test[3]) {
      ASSERT_OK(bound.GetVarchar("c2", &val));
      ASSERT_EQ(*test[3], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c2"));
    }
  };

  for (const auto& test : tests) {
    check(test, true);
    check(test, false);
  }
}

namespace {

void CheckSerializationFunctions(const PartitionSchemaPB& pb,
                                 const PartitionSchema& partition_schema,
                                 const Schema& schema) {
  PartitionSchemaPB pb1;
  ASSERT_OK(partition_schema.ToPB(schema, &pb1));

  // Compares original protobuf message to encoded protobuf message.
  MessageDifferencer::Equals(pb, pb1);

  PartitionSchema partition_schema1;
  ASSERT_OK(PartitionSchema::FromPB(pb1, schema, &partition_schema1));
  ASSERT_EQ(partition_schema, partition_schema1);
}

} // anonymous namespace

TEST_F(PartitionTest, TestVaryingHashSchemasPerRange) {
  // CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARTITION BY [HASH BUCKET (a, c), HASH BUCKET (b), RANGE (a, b, c)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  // Table-wide hash schema defined below, 3 by 2 buckets so 6 total.
  AddHashDimension(&schema_builder, { "a", "c" }, 3, 0);
  AddHashDimension(&schema_builder, { "b" }, 2, 0);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));
  CheckSerializationFunctions(schema_builder, partition_schema, schema);

  ASSERT_EQ("HASH (a, c) PARTITIONS 3, HASH (b) PARTITIONS 2, RANGE (a, b, c)",
            partition_schema.DebugString(schema));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds;
  vector<PartitionSchema::HashSchema> range_hash_schemas;
  vector<pair<pair<KuduPartialRow, KuduPartialRow>,
              PartitionSchema::HashSchema>> bounds_with_hash_schemas;

  { // [(a1, _, c1), (a2, _, c2))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a1"));
    ASSERT_OK(lower.SetStringCopy("c", "c1"));
    ASSERT_OK(upper.SetStringCopy("a", "a2"));
    ASSERT_OK(upper.SetStringCopy("c", "c2"));
    PartitionSchema::HashSchema hash_schema_4_buckets = {{{ColumnId(0)}, 4, 0}};
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(hash_schema_4_buckets);
    bounds_with_hash_schemas.emplace_back(make_pair(std::move(lower), std::move(upper)),
                                          std::move(hash_schema_4_buckets));
  }

  { // [(a3, b3, _), (a4, b4, _))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a3"));
    ASSERT_OK(lower.SetStringCopy("b", "b3"));
    ASSERT_OK(upper.SetStringCopy("a", "a4"));
    ASSERT_OK(upper.SetStringCopy("b", "b4"));
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(partition_schema.hash_schema());
    // Use the table-wide hash schema for this range.
    bounds_with_hash_schemas.emplace_back(make_pair(std::move(lower), std::move(upper)),
                                          partition_schema.hash_schema());
  }

  { // [(a5, b5, _), (a6, _, c6))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a5"));
    ASSERT_OK(lower.SetStringCopy("b", "b5"));
    ASSERT_OK(upper.SetStringCopy("a", "a6"));
    ASSERT_OK(upper.SetStringCopy("c", "c6"));
    PartitionSchema::HashSchema hash_schema_2_buckets_by_3 = {
        {{ColumnId(0)}, 2, 0},
        {{ColumnId(1)}, 3, 0}
    };
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(hash_schema_2_buckets_by_3);
    bounds_with_hash_schemas.emplace_back(make_pair(std::move(lower), std::move(upper)),
                                          std::move(hash_schema_2_buckets_by_3));
  }

  const auto check_partitions = [](const vector<Partition>& partitions) {
    ASSERT_EQ(16, partitions.size());

    ASSERT_EQ(1, partitions[0].hash_buckets().size());
    EXPECT_EQ(0, partitions[0].hash_buckets()[0]);
    EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[0].begin().range_key());
    EXPECT_EQ(string("a2\0\0\0\0c2", 8), partitions[0].end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "a1\0\0\0\0c1", 12),
              partitions[0].begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "a2\0\0\0\0c2", 12),
              partitions[0].end().ToString());

    ASSERT_EQ(1, partitions[1].hash_buckets().size());
    EXPECT_EQ(1, partitions[1].hash_buckets()[0]);
    EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[1].begin().range_key());
    EXPECT_EQ(string("a2\0\0\0\0c2", 8), partitions[1].end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "a1\0\0\0\0c1", 12),
              partitions[1].begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "a2\0\0\0\0c2", 12),
              partitions[1].end().ToString());

    ASSERT_EQ(1, partitions[2].hash_buckets().size());
    EXPECT_EQ(2, partitions[2].hash_buckets()[0]);
    EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[2].begin().range_key());
    EXPECT_EQ(string("a2\0\0\0\0c2", 8), partitions[2].end().range_key());
    EXPECT_EQ(string("\0\0\0\2" "a1\0\0\0\0c1", 12),
              partitions[2].begin().ToString());
    EXPECT_EQ(string("\0\0\0\2" "a2\0\0\0\0c2", 12),
              partitions[2].end().ToString());

    ASSERT_EQ(1, partitions[3].hash_buckets().size());
    EXPECT_EQ(3, partitions[3].hash_buckets()[0]);
    EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[3].begin().range_key());
    EXPECT_EQ(string("a2\0\0\0\0c2", 8), partitions[3].end().range_key());
    EXPECT_EQ(string("\0\0\0\3" "a1\0\0\0\0c1", 12),
              partitions[3].begin().ToString());
    EXPECT_EQ(string("\0\0\0\3" "a2\0\0\0\0c2", 12),
              partitions[3].end().ToString());

    ASSERT_EQ(2, partitions[4].hash_buckets().size());
    EXPECT_EQ(0, partitions[4].hash_buckets()[0]);
    EXPECT_EQ(0, partitions[4].hash_buckets()[1]);
    EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[4].begin().range_key());
    EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[4].end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a3\0\0b3\0\0", 16),
              partitions[4].begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a4\0\0b4\0\0", 16),
              partitions[4].end().ToString());

    ASSERT_EQ(2, partitions[5].hash_buckets().size());
    EXPECT_EQ(0, partitions[5].hash_buckets()[0]);
    EXPECT_EQ(1, partitions[5].hash_buckets()[1]);
    EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[5].begin().range_key());
    EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[5].end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a3\0\0b3\0\0", 16),
              partitions[5].begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a4\0\0b4\0\0", 16),
              partitions[5].end().ToString());

    ASSERT_EQ(2, partitions[6].hash_buckets().size());
    EXPECT_EQ(1, partitions[6].hash_buckets()[0]);
    EXPECT_EQ(0, partitions[6].hash_buckets()[1]);
    EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[6].begin().range_key());
    EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[6].end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a3\0\0b3\0\0", 16),
              partitions[6].begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a4\0\0b4\0\0", 16),
              partitions[6].end().ToString());

    ASSERT_EQ(2, partitions[7].hash_buckets().size());
    EXPECT_EQ(1, partitions[7].hash_buckets()[0]);
    EXPECT_EQ(1, partitions[7].hash_buckets()[1]);
    EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[7].begin().range_key());
    EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[7].end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a3\0\0b3\0\0", 16),
              partitions[7].begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a4\0\0b4\0\0", 16),
              partitions[7].end().ToString());

    ASSERT_EQ(2, partitions[8].hash_buckets().size());
    EXPECT_EQ(2, partitions[8].hash_buckets()[0]);
    EXPECT_EQ(0, partitions[8].hash_buckets()[1]);
    EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[8].begin().range_key());
    EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[8].end().range_key());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\0" "a3\0\0b3\0\0", 16),
              partitions[8].begin().ToString());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\0" "a4\0\0b4\0\0", 16),
              partitions[8].end().ToString());

    ASSERT_EQ(2, partitions[9].hash_buckets().size());
    EXPECT_EQ(2, partitions[9].hash_buckets()[0]);
    EXPECT_EQ(1, partitions[9].hash_buckets()[1]);
    EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[9].begin().range_key());
    EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[9].end().range_key());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\1" "a3\0\0b3\0\0", 16),
              partitions[9].begin().ToString());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\1" "a4\0\0b4\0\0", 16),
              partitions[9].end().ToString());

    ASSERT_EQ(2, partitions[10].hash_buckets().size());
    EXPECT_EQ(0, partitions[10].hash_buckets()[0]);
    EXPECT_EQ(0, partitions[10].hash_buckets()[1]);
    EXPECT_EQ(string("a5\0\0b5\0\0", 8),
              partitions[10].begin().range_key());
    EXPECT_EQ(string("a6\0\0\0\0c6", 8),
              partitions[10].end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a5\0\0b5\0\0", 16),
              partitions[10].begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a6\0\0\0\0c6", 16),
              partitions[10].end().ToString());

    ASSERT_EQ(2, partitions[11].hash_buckets().size());
    EXPECT_EQ(0, partitions[11].hash_buckets()[0]);
    EXPECT_EQ(1, partitions[11].hash_buckets()[1]);
    EXPECT_EQ(string("a5\0\0b5\0\0", 8),partitions[11].begin().range_key());
    EXPECT_EQ(string("a6\0\0\0\0c6", 8),partitions[11].end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a5\0\0b5\0\0", 16),
              partitions[11].begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a6\0\0\0\0c6", 16),
              partitions[11].end().ToString());

    ASSERT_EQ(2, partitions[12].hash_buckets().size());
    EXPECT_EQ(0, partitions[12].hash_buckets()[0]);
    EXPECT_EQ(2, partitions[12].hash_buckets()[1]);
    EXPECT_EQ(string("a5\0\0b5\0\0", 8), partitions[12].begin().range_key());
    EXPECT_EQ(string("a6\0\0\0\0c6", 8), partitions[12].end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\2" "a5\0\0b5\0\0", 16),
              partitions[12].begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\2" "a6\0\0\0\0c6", 16),
              partitions[12].end().ToString());

    ASSERT_EQ(2, partitions[13].hash_buckets().size());
    EXPECT_EQ(1, partitions[13].hash_buckets()[0]);
    EXPECT_EQ(0, partitions[13].hash_buckets()[1]);
    EXPECT_EQ(string("a5\0\0b5\0\0", 8), partitions[13].begin().range_key());
    EXPECT_EQ(string("a6\0\0\0\0c6", 8), partitions[13].end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a5\0\0b5\0\0", 16),
              partitions[13].begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a6\0\0\0\0c6", 16),
              partitions[13].end().ToString());

    ASSERT_EQ(2, partitions[14].hash_buckets().size());
    EXPECT_EQ(1, partitions[14].hash_buckets()[0]);
    EXPECT_EQ(1, partitions[14].hash_buckets()[1]);
    EXPECT_EQ(string("a5\0\0b5\0\0", 8), partitions[14].begin().range_key());
    EXPECT_EQ(string("a6\0\0\0\0c6", 8), partitions[14].end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a5\0\0b5\0\0", 16),
              partitions[14].begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a6\0\0\0\0c6", 16),
              partitions[14].end().ToString());

    ASSERT_EQ(2, partitions[15].hash_buckets().size());
    EXPECT_EQ(1, partitions[15].hash_buckets()[0]);
    EXPECT_EQ(2, partitions[15].hash_buckets()[1]);
    EXPECT_EQ(string("a5\0\0b5\0\0", 8), partitions[15].begin().range_key());
    EXPECT_EQ(string("a6\0\0\0\0c6", 8), partitions[15].end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\2" "a5\0\0b5\0\0", 16),
              partitions[15].begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\2" "a6\0\0\0\0c6", 16),
              partitions[15].end().ToString());
  };

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(
      {}, bounds, range_hash_schemas, schema_ptr, &partitions));
  NO_FATALS(check_partitions(partitions));

  bounds.clear();
  range_hash_schemas.clear();
  partitions.clear();

  // Using std::random_shuffle to insert bounds and their hash schemas out of
  // sorted order, yet resulting partitions will still be the same.
  std::mt19937 gen(SeedRandom());
  std::shuffle(bounds_with_hash_schemas.begin(), bounds_with_hash_schemas.end(), gen);

  for (const auto& bounds_and_schema : bounds_with_hash_schemas) {
    bounds.emplace_back(bounds_and_schema.first);
    range_hash_schemas.emplace_back(bounds_and_schema.second);
  }

  ASSERT_OK(partition_schema.CreatePartitions(
      {}, bounds, range_hash_schemas, schema_ptr, &partitions));
  NO_FATALS(check_partitions(partitions));

  // Not clearing bounds or range_hash_schemas, adding a split row to test
  // incompatibility.
  vector<KuduPartialRow> splits;
  { // split: (a1, _, c12)
    KuduPartialRow split(&schema);
    ASSERT_OK(split.SetStringCopy("a", "a1"));
    ASSERT_OK(split.SetStringCopy("c", "c12"));
    splits.emplace_back(std::move(split));
  }

  // Expecting Status::InvalidArgument() due to 'splits' and schemas within
  // 'range_hash_schemas' being defined at the same time.
  {
    const auto s = partition_schema.CreatePartitions(
        splits, bounds, range_hash_schemas, schema_ptr, &partitions);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Both 'split_rows' and 'range_hash_schemas' "
                        "cannot be populated at the same time");
  }

  // Adding another schema to range_hash_schemas to trigger
  // Status::InvalidArgument() due to 'bounds and 'range_hash_schema' not being
  // the same size.
  {
    range_hash_schemas.push_back({});
    const auto s = partition_schema.CreatePartitions(
        {}, bounds, range_hash_schemas, schema_ptr, &partitions);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "4 vs 3: per range hash schemas and range bounds "
                        "must have the same size");
  }
}

TEST_F(PartitionTest, CustomHashSchemasPerRangeOnly) {
  // CREATE TABLE t (a STRING, b STRING, PRIMARY KEY (a, b)) RANGE (a, b)
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING), ColumnSchema("b", STRING) },
                { ColumnId(0), ColumnId(1) }, 2));
  Schema& schema = *schema_ptr.get();

  // No table-wide hash bucket schema.
  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));
  CheckSerializationFunctions(schema_builder, partition_schema, schema);

  ASSERT_EQ("RANGE (a, b)", partition_schema.DebugString(schema));

  typedef pair<KuduPartialRow, KuduPartialRow> RangeBound;
  vector<RangeBound> bounds;
  vector<PartitionSchema::HashSchema> range_hash_schemas;
  vector<pair<RangeBound, PartitionSchema::HashSchema>>
      bounds_with_hash_schemas;

  // [(a1, b1), (a2, b2))
  {
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringNoCopy("a", "a1"));
    ASSERT_OK(lower.SetStringNoCopy("b", "b1"));
    ASSERT_OK(upper.SetStringNoCopy("a", "a2"));
    ASSERT_OK(upper.SetStringNoCopy("b", "b2"));
    PartitionSchema::HashSchema hash_schema_2_buckets =
        { { { ColumnId(0) }, 2, 0 } };
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(hash_schema_2_buckets);
    bounds_with_hash_schemas.emplace_back(
        make_pair(std::move(lower), std::move(upper)),
        std::move(hash_schema_2_buckets));
  }

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(
      {}, bounds, range_hash_schemas, schema_ptr, &partitions));

  ASSERT_EQ(2, partitions.size());

  {
    const auto& p = partitions[0];
    ASSERT_EQ(1, p.hash_buckets().size());
    EXPECT_EQ(0, p.hash_buckets()[0]);
    EXPECT_EQ(string("a1\0\0b1", 6), p.begin().range_key());
    EXPECT_EQ(string("a2\0\0b2", 6), p.end().range_key());
  }

  {
    const auto& p = partitions[1];
    ASSERT_EQ(1, p.hash_buckets().size());
    EXPECT_EQ(1, p.hash_buckets()[0]);
    EXPECT_EQ(string("a1\0\0b1", 6), p.begin().range_key());
    EXPECT_EQ(string("a2\0\0b2", 6), p.end().range_key());
  }
}

TEST_F(PartitionTest, TestVaryingHashSchemasPerUnboundedRanges) {
  // CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARTITION BY [HASH BUCKET (b), RANGE (a, b, c)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  // Table-wide hash schema defined below.
  AddHashDimension(&schema_builder, { "b" }, 2, 0);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));
  CheckSerializationFunctions(schema_builder, partition_schema, schema);

  ASSERT_EQ("HASH (b) PARTITIONS 2, RANGE (a, b, c)",
            partition_schema.DebugString(schema));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds;
  vector<PartitionSchema::HashSchema> range_hash_schemas;

  { // [(_, _, _), (a1, _, c1))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(upper.SetStringCopy("a", "a1"));
    ASSERT_OK(upper.SetStringCopy("c", "c1"));
    PartitionSchema::HashSchema hash_schema_4_buckets = {{{ColumnId(0)}, 4, 0}};
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(hash_schema_4_buckets);
  }

  { // [(a2, b2, _), (a3, b3, _))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a2"));
    ASSERT_OK(lower.SetStringCopy("b", "b2"));
    ASSERT_OK(upper.SetStringCopy("a", "a3"));
    ASSERT_OK(upper.SetStringCopy("b", "b3"));
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(PartitionSchema::HashSchema());
  }

  { // [(a4, b4, _), (_, _, _))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a4"));
    ASSERT_OK(lower.SetStringCopy("b", "b4"));
    PartitionSchema::HashSchema hash_schema_2_buckets_by_3 = {
        {{ColumnId(0)}, 2, 0},
        {{ColumnId(2)}, 3, 0}
    };
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(hash_schema_2_buckets_by_3);
  }

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({}, bounds, range_hash_schemas,
                                              schema_ptr, &partitions));
  ASSERT_EQ(11, partitions.size());
  // Partitions below sorted by range, can verify that the partition keyspace is filled by checking
  // that the start key of the first partition and the end key of the last partition is cleared.

  ASSERT_EQ(1, partitions[0].hash_buckets().size());
  EXPECT_EQ(0, partitions[0].hash_buckets()[0]);
  EXPECT_EQ("", partitions[0].begin().range_key());
  EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[0].end().range_key());
  EXPECT_EQ("", partitions[0].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "a1\0\0\0\0c1", 12), partitions[0].end().ToString());

  ASSERT_EQ(1, partitions[1].hash_buckets().size());
  EXPECT_EQ(1, partitions[1].hash_buckets()[0]);
  EXPECT_EQ("", partitions[1].begin().range_key());
  EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[1].end().range_key());
  EXPECT_EQ(string("\0\0\0\1", 4),partitions[1].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "a1\0\0\0\0c1", 12), partitions[1].end().ToString());

  ASSERT_EQ(1, partitions[2].hash_buckets().size());
  EXPECT_EQ(2, partitions[2].hash_buckets()[0]);
  EXPECT_EQ("", partitions[2].begin().range_key());
  EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[2].end().range_key());
  EXPECT_EQ(string("\0\0\0\2", 4), partitions[2].begin().ToString());
  EXPECT_EQ(string("\0\0\0\2" "a1\0\0\0\0c1", 12), partitions[2].end().ToString());

  ASSERT_EQ(1, partitions[3].hash_buckets().size());
  EXPECT_EQ(3, partitions[3].hash_buckets()[0]);
  EXPECT_EQ("", partitions[3].begin().range_key());
  EXPECT_EQ(string("a1\0\0\0\0c1", 8), partitions[3].end().range_key());
  EXPECT_EQ(string("\0\0\0\3", 4), partitions[3].begin().ToString());
  EXPECT_EQ(string("\0\0\0\3" "a1\0\0\0\0c1", 12), partitions[3].end().ToString());

  ASSERT_EQ(0, partitions[4].hash_buckets().size());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[4].begin().range_key());
  EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[4].end().range_key());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[4].begin().ToString());
  EXPECT_EQ(string("a3\0\0b3\0\0", 8), partitions[4].end().ToString());

  ASSERT_EQ(2, partitions[5].hash_buckets().size());
  EXPECT_EQ(0, partitions[5].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[5].hash_buckets()[1]);
  EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[5].begin().range_key());
  EXPECT_EQ("", partitions[5].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a4\0\0b4\0\0", 16), partitions[5].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1", 8), partitions[5].end().ToString());

  ASSERT_EQ(2, partitions[6].hash_buckets().size());
  EXPECT_EQ(0, partitions[6].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[6].hash_buckets()[1]);
  EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[6].begin().range_key());
  EXPECT_EQ("", partitions[6].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a4\0\0b4\0\0", 16),partitions[6].begin().ToString());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\2", 8), partitions[6].end().ToString());

  ASSERT_EQ(2, partitions[7].hash_buckets().size());
  EXPECT_EQ(0, partitions[7].hash_buckets()[0]);
  EXPECT_EQ(2, partitions[7].hash_buckets()[1]);
  EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[7].begin().range_key());
  EXPECT_EQ("", partitions[7].end().range_key());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\2" "a4\0\0b4\0\0", 16), partitions[7].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[7].end().ToString());

  ASSERT_EQ(2, partitions[8].hash_buckets().size());
  EXPECT_EQ(1, partitions[8].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[8].hash_buckets()[1]);
  EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[8].begin().range_key());
  EXPECT_EQ("", partitions[8].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a4\0\0b4\0\0", 16), partitions[8].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1", 8), partitions[8].end().ToString());

  ASSERT_EQ(2, partitions[9].hash_buckets().size());
  EXPECT_EQ(1, partitions[9].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[9].hash_buckets()[1]);
  EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[9].begin().range_key());
  EXPECT_EQ("", partitions[9].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a4\0\0b4\0\0", 16),partitions[9].begin().ToString());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\2", 8), partitions[9].end().ToString());

  ASSERT_EQ(2, partitions[10].hash_buckets().size());
  EXPECT_EQ(1, partitions[10].hash_buckets()[0]);
  EXPECT_EQ(2, partitions[10].hash_buckets()[1]);
  EXPECT_EQ(string("a4\0\0b4\0\0", 8), partitions[10].begin().range_key());
  EXPECT_EQ("", partitions[10].end().range_key());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\2" "a4\0\0b4\0\0", 16), partitions[10].begin().ToString());
  EXPECT_EQ("", partitions[10].end().ToString());
}

TEST_F(PartitionTest, NoHashSchemasForLastUnboundedRange) {
  // CREATE TABLE t (a VARCHAR, b VARCHAR, PRIMARY KEY (a, b))
  // PARTITION BY [HASH BUCKET (b), RANGE (a, b)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING) },
                { ColumnId(0), ColumnId(1) }, 2));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  // Table-wide hash schema defined below.
  AddHashDimension(&schema_builder, { "b" }, 2, 0);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));
  CheckSerializationFunctions(schema_builder, partition_schema, schema);

  ASSERT_EQ("HASH (b) PARTITIONS 2, RANGE (a, b)",
            partition_schema.DebugString(schema));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds;
  vector<PartitionSchema::HashSchema> range_hash_schemas;

  // [(_, _), (a1, _))
  {
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(upper.SetStringCopy("a", "a1"));
    PartitionSchema::HashSchema hash_schema_3_buckets = {{{ColumnId(0)}, 3, 0}};
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(hash_schema_3_buckets);
  }

  // [(a2, _), (a3, _))
  {
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a2"));
    ASSERT_OK(upper.SetStringCopy("a", "a3"));
    bounds.emplace_back(lower, upper);
    PartitionSchema::HashSchema hash_schema_3_buckets_by_2 = {
        {{ColumnId(0)}, 3, 0},
        {{ColumnId(1)}, 2, 0}
    };
    range_hash_schemas.emplace_back(hash_schema_3_buckets_by_2);
  }

  // [(a4, _), (_, _))
  {
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a4"));
    bounds.emplace_back(lower, upper);
    range_hash_schemas.emplace_back(PartitionSchema::HashSchema());
  }

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(
      {}, bounds, range_hash_schemas, schema_ptr, &partitions));
  ASSERT_EQ(10, partitions.size());

  {
    const auto& p = partitions[0];
    ASSERT_EQ(1, p.hash_buckets().size());
    EXPECT_EQ(0, p.hash_buckets()[0]);
    EXPECT_EQ("", p.begin().range_key());
    EXPECT_EQ(string("a1\0\0", 4), p.end().range_key());
    EXPECT_EQ("", p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "a1\0\0c1", 8), p.end().ToString());
  }
  {
    const auto& p = partitions[1];
    ASSERT_EQ(1, p.hash_buckets().size());
    EXPECT_EQ(1, p.hash_buckets()[0]);
    EXPECT_EQ("", p.begin().range_key());
    EXPECT_EQ(string("a1\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\1", 4), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "a1\0\0", 8), p.end().ToString());
  }
  {
    const auto& p = partitions[2];
    ASSERT_EQ(1, p.hash_buckets().size());
    EXPECT_EQ(2, p.hash_buckets()[0]);
    EXPECT_EQ("", p.begin().range_key());
    EXPECT_EQ(string("a1\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\2", 4), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\2" "a1\0\0", 8), p.end().ToString());
  }
  {
    const auto& p = partitions[3];
    ASSERT_EQ(2, p.hash_buckets().size());
    EXPECT_EQ(0, p.hash_buckets()[0]);
    EXPECT_EQ(0, p.hash_buckets()[1]);
    EXPECT_EQ(string("a2\0\0", 4), p.begin().range_key());
    EXPECT_EQ(string("a3\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a2\0\0", 12), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a3\0\0", 12), p.end().ToString());
  }
  {
    const auto& p = partitions[4];
    ASSERT_EQ(2, p.hash_buckets().size());
    EXPECT_EQ(0, p.hash_buckets()[0]);
    EXPECT_EQ(1, p.hash_buckets()[1]);
    EXPECT_EQ(string("a2\0\0", 4), p.begin().range_key());
    EXPECT_EQ(string("a3\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a2\0\0", 12), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a3\0\0", 12), p.end().ToString());
  }
  {
    const auto& p = partitions[5];
    ASSERT_EQ(2, p.hash_buckets().size());
    EXPECT_EQ(1, p.hash_buckets()[0]);
    EXPECT_EQ(0, p.hash_buckets()[1]);
    EXPECT_EQ(string("a2\0\0", 4), p.begin().range_key());
    EXPECT_EQ(string("a3\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a2\0\0", 12), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a3\0\0", 12), p.end().ToString());
  }
  {
    const auto& p = partitions[6];
    ASSERT_EQ(2, p.hash_buckets().size());
    EXPECT_EQ(1, p.hash_buckets()[0]);
    EXPECT_EQ(1, p.hash_buckets()[1]);
    EXPECT_EQ(string("a2\0\0", 4), p.begin().range_key());
    EXPECT_EQ(string("a3\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a2\0\0", 12), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a3\0\0", 12), p.end().ToString());
  }
  {
    const auto& p = partitions[7];
    ASSERT_EQ(2, p.hash_buckets().size());
    EXPECT_EQ(2, p.hash_buckets()[0]);
    EXPECT_EQ(0, p.hash_buckets()[1]);
    EXPECT_EQ(string("a2\0\0", 4), p.begin().range_key());
    EXPECT_EQ(string("a3\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\0" "a2\0\0", 12), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\0" "a3\0\0", 12), p.end().ToString());
  }
  {
    const auto& p = partitions[8];
    ASSERT_EQ(2, p.hash_buckets().size());
    EXPECT_EQ(2, p.hash_buckets()[0]);
    EXPECT_EQ(1, p.hash_buckets()[1]);
    EXPECT_EQ(string("a2\0\0", 4), p.begin().range_key());
    EXPECT_EQ(string("a3\0\0", 4), p.end().range_key());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\1" "a2\0\0", 12), p.begin().ToString());
    EXPECT_EQ(string("\0\0\0\2" "\0\0\0\1" "a3\0\0", 12), p.end().ToString());
  }
  {
    const auto& p = partitions[9];
    ASSERT_EQ(0, p.hash_buckets().size());
    EXPECT_EQ(string("a4\0\0", 4), p.begin().range_key());
    EXPECT_EQ("", p.end().range_key());
    EXPECT_EQ(string("a4\0\0", 4), p.begin().ToString());
    EXPECT_EQ("", p.end().ToString());
  }
}

TEST_F(PartitionTest, TestPartitionSchemaPB) {
  // CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARTITION BY [HASH BUCKET (b), RANGE (a, b, c)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB pb;
  // Table-wide hash schema defined below.
  AddHashDimension(&pb, { "b" }, 2, 0);

  // [(a0, _, c0), (a0, _, c1))
  {
    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a0"));
    ASSERT_OK(lower.SetStringCopy("c", "c0"));
    ASSERT_OK(upper.SetStringCopy("a", "a0"));
    ASSERT_OK(upper.SetStringCopy("c", "c1"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    auto* hash_dimension = range->add_hash_schema();
    hash_dimension->add_columns()->set_name("a");
    hash_dimension->set_num_buckets(4);
  }

  // [(a1, _, c2), (a1, _, c3))
  {
    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a1"));
    ASSERT_OK(lower.SetStringCopy("c", "c2"));
    ASSERT_OK(upper.SetStringCopy("a", "a1"));
    ASSERT_OK(upper.SetStringCopy("c", "c3"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    {
      auto* hash_dimension = range->add_hash_schema();
      hash_dimension->add_columns()->set_name("a");
      hash_dimension->set_num_buckets(2);
    }
    {
      auto* hash_dimension = range->add_hash_schema();
      hash_dimension->add_columns()->set_name("b");
      hash_dimension->set_num_buckets(3);
    }
  }

  // [(a2, _, c4), (a2, _, c5))
  {
    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a2"));
    ASSERT_OK(lower.SetStringCopy("c", "c4"));
    ASSERT_OK(upper.SetStringCopy("a", "a2"));
    ASSERT_OK(upper.SetStringCopy("c", "c5"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    // Empty 'hash_schema' field overrides the table-wide hash schema,
    // meaning 'no hash bucketing for the range'.
  }

  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  // Check fields of 'partition_schema' to verify decoder function.
  ASSERT_EQ(1, partition_schema.hash_schema().size());
  const auto& ranges_with_hash_schemas = partition_schema.ranges_with_hash_schemas();
  ASSERT_EQ(3, ranges_with_hash_schemas.size());

  EXPECT_EQ(string("a0\0\0\0\0c0", 8), ranges_with_hash_schemas[0].lower);
  EXPECT_EQ(string("a0\0\0\0\0c1", 8), ranges_with_hash_schemas[0].upper);
  EXPECT_EQ(1, ranges_with_hash_schemas[0].hash_schema.size());

  const auto& range1_hash_schema = ranges_with_hash_schemas[0].hash_schema[0];
  EXPECT_EQ(1, range1_hash_schema.column_ids.size());
  EXPECT_EQ(0, range1_hash_schema.column_ids[0]);
  EXPECT_EQ(4, range1_hash_schema.num_buckets);

  EXPECT_EQ(string("a1\0\0\0\0c2", 8), ranges_with_hash_schemas[1].lower);
  EXPECT_EQ(string("a1\0\0\0\0c3", 8), ranges_with_hash_schemas[1].upper);
  EXPECT_EQ(2, ranges_with_hash_schemas[1].hash_schema.size());

  const auto& range2_hash_schema_1 = ranges_with_hash_schemas[1].hash_schema[0];
  EXPECT_EQ(1, range2_hash_schema_1.column_ids.size());
  EXPECT_EQ(0, range2_hash_schema_1.column_ids[0]);
  EXPECT_EQ(2, range2_hash_schema_1.num_buckets);

  const auto& range2_hash_schema_2 = ranges_with_hash_schemas[1].hash_schema[1];
  EXPECT_EQ(1, range2_hash_schema_2.column_ids.size());
  EXPECT_EQ(1, range2_hash_schema_2.column_ids[0]);
  EXPECT_EQ(3, range2_hash_schema_2.num_buckets);

  EXPECT_EQ(string("a2\0\0\0\0c4", 8), ranges_with_hash_schemas[2].lower);
  EXPECT_EQ(string("a2\0\0\0\0c5", 8), ranges_with_hash_schemas[2].upper);
  EXPECT_EQ(0, ranges_with_hash_schemas[2].hash_schema.size());

  CheckSerializationFunctions(pb, partition_schema, schema);
}

TEST_F(PartitionTest, TestMalformedPartitionSchemaPB) {
  // CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARTITION BY [RANGE (a, b, c)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  // Testing that only a pair of range bounds is allowed.
  {
    PartitionSchemaPB pb;
    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    KuduPartialRow extra(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a0"));
    ASSERT_OK(upper.SetStringCopy("a", "a1"));
    ASSERT_OK(extra.SetStringCopy("a", "a2"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, extra);

    PartitionSchema partition_schema;
    auto s = PartitionSchema::FromPB(pb, schema, &partition_schema);
    ASSERT_EQ("Invalid argument: 3 ops were provided; "
              "only two ops are expected for this pair of range bounds",
              s.ToString());
  }

  // Testing that no split rows are allowed along with ranges with custom
  // hash schema.
  {
    PartitionSchemaPB pb;
    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow split(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(split.SetStringCopy("a", "a0"));
    ASSERT_OK(upper.SetStringCopy("a", "a1"));
    encoder.Add(RowOperationsPB::SPLIT_ROW, split);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    PartitionSchema partition_schema;
    auto s = PartitionSchema::FromPB(pb, schema, &partition_schema);
    ASSERT_EQ("Invalid argument: Illegal row operation type in request: 4",
              s.ToString());
  }

  // Testing that 2nd bound is either RANGE_UPPER_BOUND or INCLUSIVE_RANGE_UPPER_BOUND.
  {
    PartitionSchemaPB pb;
    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a0"));
    ASSERT_OK(upper.SetStringCopy("a", "a1"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::SPLIT_ROW, upper);

    PartitionSchema partition_schema;
    auto s = PartitionSchema::FromPB(pb, schema, &partition_schema);
    ASSERT_EQ("Invalid argument: missing upper range bound in request",
              s.ToString());
  }
}

TEST_F(PartitionTest, TestOverloadedEqualsOperator) {
  // CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARTITION BY [RANGE (a, b, c)];
  SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  PartitionSchemaPB schema_builder_1;
  PartitionSchema partition_schema_1;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder_1, schema, &partition_schema_1));

  // Same object.
  ASSERT_EQ(partition_schema, partition_schema);
  ASSERT_EQ(partition_schema_1, partition_schema_1);

  // Both schemas should be identical.
  ASSERT_EQ(partition_schema, partition_schema_1);

  // Clears the range schema of 'schema_builder_1'.
  SetRangePartitionComponent(&schema_builder_1, {});
  ASSERT_OK(PartitionSchema::FromPB(schema_builder_1, schema, &partition_schema_1));
  ASSERT_NE(partition_schema, partition_schema_1);

  // Resets range schema so both will be equal again.
  SetRangePartitionComponent(&schema_builder_1, {"a", "b", "c"});

  // Table wide hash schemas are different.
  AddHashDimension(&schema_builder, { "a" }, 2, 0);
  AddHashDimension(&schema_builder_1, { "b" }, 2, 0);
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));
  ASSERT_OK(PartitionSchema::FromPB(schema_builder_1, schema, &partition_schema_1));
  ASSERT_NE(partition_schema, partition_schema_1);

  // Resets table wide hash schemas so both will be equal again.
  schema_builder_1.clear_hash_schema();
  AddHashDimension(&schema_builder_1, { "a" }, 2, 0);

  // Different sizes of field 'ranges_with_hash_schemas_'
  // [(a, _, _), (b, _, _))
  {
    auto* range = schema_builder_1.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a"));
    ASSERT_OK(upper.SetStringCopy("a", "b"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    auto* hash_dimension = range->add_hash_schema();
    hash_dimension->add_columns()->set_name("a");
    hash_dimension->set_num_buckets(4);
  }

  ASSERT_OK(PartitionSchema::FromPB(schema_builder_1, schema, &partition_schema_1));
  ASSERT_NE(partition_schema, partition_schema_1);

  // Different custom hash bucket schema but same range bounds.
  // [(a, _, _), (b, _, _))
  {
    auto* range = schema_builder.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a"));
    ASSERT_OK(upper.SetStringCopy("a", "b"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    auto* hash_dimension = range->add_hash_schema();
    hash_dimension->add_columns()->set_name("a");
    hash_dimension->set_num_buckets(2);
  }

  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));
  ASSERT_NE(partition_schema, partition_schema_1);

  schema_builder.clear_custom_hash_schema_ranges();

  // Different range bounds but same custom hash bucket schema.
  // [(a, _, _), (c, _, _))
  {
    auto* range = schema_builder.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a"));
    ASSERT_OK(upper.SetStringCopy("a", "c"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    auto* hash_dimension = range->add_hash_schema();
    hash_dimension->add_columns()->set_name("a");
    hash_dimension->set_num_buckets(4);
  }

  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));
  ASSERT_NE(partition_schema, partition_schema_1);
}

// A test scenario to verify functionality of the
// PartitionSchema::HasCustomHashSchemas() method.
TEST_F(PartitionTest, HasCustomHashSchemasMethod) {
  const SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("a", STRING),
                        ColumnSchema("b", STRING),
                        ColumnSchema("c", STRING) },
                      { ColumnId(0), ColumnId(1), ColumnId(2) }, 3));
  Schema& schema = *schema_ptr.get();


  // No hash schema (even table-wide) case.
  {
    PartitionSchemaPB pb;
    PartitionSchema partition_schema;
    ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));
    ASSERT_FALSE(partition_schema.HasCustomHashSchemas());
  }

  // Table-wide hash schema.
  {
    PartitionSchemaPB pb;
    AddHashDimension(&pb, { "b" }, 2, 0);
    PartitionSchema partition_schema;
    ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));
    ASSERT_FALSE(partition_schema.HasCustomHashSchemas());
  }

  // No table-wide schema, just a range with custom hash schema.
  {
    PartitionSchemaPB pb;
    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a0"));
    ASSERT_OK(lower.SetStringCopy("c", "c0"));
    ASSERT_OK(upper.SetStringCopy("a", "a0"));
    ASSERT_OK(upper.SetStringCopy("c", "c1"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    auto* hash_dimension = range->add_hash_schema();
    hash_dimension->add_columns()->set_name("a");
    hash_dimension->set_num_buckets(2);

    PartitionSchema partition_schema;
    ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));
    ASSERT_TRUE(partition_schema.HasCustomHashSchemas());
  }

  // Table-wide hash schema and one range with custom hash schema.
  {
    PartitionSchemaPB pb;
    AddHashDimension(&pb, { "a" }, 2, 0);

    auto* range = pb.add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("a", "a0"));
    ASSERT_OK(lower.SetStringCopy("c", "c0"));
    ASSERT_OK(upper.SetStringCopy("a", "a1"));
    ASSERT_OK(upper.SetStringCopy("c", "c1"));
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

    auto* hash_dimension = range->add_hash_schema();
    hash_dimension->add_columns()->set_name("b");
    hash_dimension->set_num_buckets(3);

    PartitionSchema partition_schema;
    ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));
    ASSERT_TRUE(partition_schema.HasCustomHashSchemas());
  }
}

} // namespace kudu
