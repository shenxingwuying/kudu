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

#include "kudu/common/encoded_key.h"

#include <cstdint>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h" // IWYU pragma: keep
#include "kudu/util/faststring.h"
#include "kudu/util/int128.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h" // IWYU pragma: keep
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;

namespace kudu {
class EncodedKeyTest;
class EncodedKeyTest_TestConstructFromEncodedString_Test;
class EncodedKeyTest_TestDecodeCompoundKeys_Test;
class EncodedKeyTest_TestDecodeSimpleKeys_Test;
} // namespace kudu

#define EXPECT_ROWKEY_EQ(schema, expected, enc_key)     \
  do { \
    SCOPED_TRACE(""); \
    EXPECT_NO_FATAL_FAILURE(ExpectRowKeyEq((schema), (expected), (enc_key))); \
  } while (0)

#define EXPECT_DECODED_KEY_EQ(type, expected, encoded_form, val) \
  do { \
    SCOPED_TRACE(""); \
    EXPECT_NO_FATAL_FAILURE(ExpectDecodedKeyEq<(type)>((expected), (encoded_form), (val))); \
  } while (0)

namespace kudu {

class EncodedKeyTest : public KuduTest {
 public:
  EncodedKeyTest() : schema_(CreateSchema()), arena_(1024) {}

  static SchemaRefPtr CreateSchema() {
    return make_scoped_refptr(new Schema({ ColumnSchema("key", UINT32) }, 1));
  }

  EncodedKey* BuildEncodedKey(int val) {
    EncodedKeyBuilder ekb(schema_.get(), &arena_);
    ekb.AddColumnKey(&val);
    return ekb.BuildEncodedKey();
  }

  // Test whether target lies within the numerical key ranges given by
  // start and end. If -1, an empty slice is used instead.
  bool InRange(int start, int end, int target) {
    arena_.Reset();
    EncodedKey* start_key = BuildEncodedKey(start);
    EncodedKey* end_key = BuildEncodedKey(end);
    EncodedKey* target_key = BuildEncodedKey(target);
    return target_key->InRange(start != -1 ? start_key->encoded_key() : Slice(),
                               end != -1 ? end_key->encoded_key() : Slice());
  }

  void ExpectRowKeyEq(const Schema& schema,
                      const string& exp_str,
                      const EncodedKey& key) {
    EXPECT_EQ(exp_str, schema.DebugEncodedRowKey(key.encoded_key(), Schema::START_KEY));
  }

  template<DataType Type>
  void ExpectDecodedKeyEq(const string& expected,
                          const Slice& encoded_form,
                          void* val) {
    SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("key", Type) }, 1));
    Schema& schema = *schema_ptr.get();
    EncodedKeyBuilder builder(&schema, &arena_);
    builder.AddColumnKey(val);
    EncodedKey* key = builder.BuildEncodedKey();
    EXPECT_ROWKEY_EQ(schema, expected, *key);
    EXPECT_EQ(encoded_form, key->encoded_key());
  }

 protected:
  SchemaRefPtr schema_;
  Arena arena_;
};

TEST_F(EncodedKeyTest, TestKeyInRange) {
  ASSERT_TRUE(InRange(-1, -1, 0));
  ASSERT_TRUE(InRange(-1, -1, 50));

  ASSERT_TRUE(InRange(-1, 30, 0));
  ASSERT_TRUE(InRange(-1, 30, 29));
  ASSERT_FALSE(InRange(-1, 30, 30));
  ASSERT_FALSE(InRange(-1, 30, 31));

  ASSERT_FALSE(InRange(10, -1, 9));
  ASSERT_TRUE(InRange(10, -1, 10));
  ASSERT_TRUE(InRange(10, -1, 11));
  ASSERT_TRUE(InRange(10, -1, 31));

  ASSERT_FALSE(InRange(10, 20, 9));
  ASSERT_TRUE(InRange(10, 20, 10));
  ASSERT_TRUE(InRange(10, 20, 19));
  ASSERT_FALSE(InRange(10, 20, 20));
  ASSERT_FALSE(InRange(10, 20, 21));
}

TEST_F(EncodedKeyTest, TestDecodeSimpleKeys) {
  {
    uint8_t val = 123;
    EXPECT_DECODED_KEY_EQ(UINT8, "(uint8 key=123)", "\x7b", &val);
  }

  {
    int8_t val = -123;
    EXPECT_DECODED_KEY_EQ(INT8, "(int8 key=-123)", "\x05", &val);
  }

  {
    uint16_t val = 12345;
    EXPECT_DECODED_KEY_EQ(UINT16, "(uint16 key=12345)", "\x30\x39", &val);
  }

  {
    int16_t val = 12345;
    EXPECT_DECODED_KEY_EQ(INT16, "(int16 key=12345)", "\xb0\x39", &val);
  }

  {
    int16_t val = -12345;
    EXPECT_DECODED_KEY_EQ(INT16, "(int16 key=-12345)", "\x4f\xc7", &val);
  }

  {
    uint32_t val = 123456;
    EXPECT_DECODED_KEY_EQ(UINT32, "(uint32 key=123456)",
                          Slice("\x00\x01\xe2\x40", 4), &val);
  }

  {
    int32_t val = -123456;
    EXPECT_DECODED_KEY_EQ(INT32, "(int32 key=-123456)", "\x7f\xfe\x1d\xc0", &val);
  }

  {
    uint64_t val = 1234567891011121314;
    EXPECT_DECODED_KEY_EQ(UINT64, "(uint64 key=1234567891011121314)",
                          "\x11\x22\x10\xf4\xb2\xd2\x30\xa2", &val);
  }

  {
    int64_t val = -1234567891011121314;
    EXPECT_DECODED_KEY_EQ(INT64, "(int64 key=-1234567891011121314)",
                          "\x6e\xdd\xef\x0b\x4d\x2d\xcf\x5e", &val);
  }

  {
    int128_t val = INT128_MAX;
    EXPECT_DECODED_KEY_EQ(INT128, "(int128 key=170141183460469231731687303715884105727)",
                          "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", &val);
  }

  {
    int128_t val = -1234567891011121314;
    EXPECT_DECODED_KEY_EQ(INT128, "(int128 key=-1234567891011121314)",
                          "\x7f\xff\xff\xff\xff\xff\xff\xff\xee\xdd\xef\x0b\x4d\x2d\xcf\x5e", &val);
  }

  {
    Slice val("aKey");
    EXPECT_DECODED_KEY_EQ(STRING, R"((string key="aKey"))", "aKey", &val);
  }
}

TEST_F(EncodedKeyTest, TestDecodeCompoundKeys) {
  {
    // Integer type compound key.
    SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("key0", UINT16),
                    ColumnSchema("key1", UINT32),
                    ColumnSchema("key2", UINT64) }, 3));
    Schema& schema = *schema_ptr.get();

    EncodedKeyBuilder builder(&schema, &arena_);
    uint16_t key0 = 12345;
    uint32_t key1 = 123456;
    uint64_t key2 = 1234567891011121314;
    builder.AddColumnKey(&key0);
    builder.AddColumnKey(&key1);
    builder.AddColumnKey(&key2);
    auto* key = builder.BuildEncodedKey();

    EXPECT_ROWKEY_EQ(schema,
                     "(uint16 key0=12345, uint32 key1=123456, uint64 key2=1234567891011121314)",
                     *key);
  }

  {
    // Mixed type compound key with STRING last.
    SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("key0", UINT16),
                    ColumnSchema("key1", STRING) }, 2));
    Schema& schema = *schema_ptr.get();
    EncodedKeyBuilder builder(&schema, &arena_);
    uint16_t key0 = 12345;
    Slice key1("aKey");
    builder.AddColumnKey(&key0);
    builder.AddColumnKey(&key1);
    auto* key = builder.BuildEncodedKey();

    EXPECT_ROWKEY_EQ(schema, R"((uint16 key0=12345, string key1="aKey"))", *key);
  }

  {
    // Mixed type compound key with STRING in the middle
    SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("key0", UINT16),
                    ColumnSchema("key1", STRING),
                    ColumnSchema("key2", UINT8) }, 3));
    Schema& schema = *schema_ptr.get();
    EncodedKeyBuilder builder(&schema, &arena_);
    uint16_t key0 = 12345;
    Slice key1("aKey");
    uint8_t key2 = 123;
    builder.AddColumnKey(&key0);
    builder.AddColumnKey(&key1);
    builder.AddColumnKey(&key2);
    auto* key = builder.BuildEncodedKey();

    EXPECT_ROWKEY_EQ(schema, R"((uint16 key0=12345, string key1="aKey", uint8 key2=123))", *key);
  }
}

TEST_F(EncodedKeyTest, TestConstructFromEncodedString) {
  EncodedKey* key = nullptr;

  {
    // Integer type compound key.
    SchemaRefPtr schema_ptr(new Schema({ ColumnSchema("key0", UINT16),
                    ColumnSchema("key1", UINT32),
                    ColumnSchema("key2", UINT64) }, 3));
    Schema& schema = *schema_ptr.get();

    // Prefix with only one full column specified
    ASSERT_OK(EncodedKey::DecodeEncodedString(schema,
                                              &arena_,
                                              Slice("\x00\x01"
                                                    "\x00\x00\x00\x02"
                                                    "\x00\x00\x00\x00\x00\x00\x00\x03",
                                                    14),
                                              &key));
    EXPECT_ROWKEY_EQ(schema, "(uint16 key0=1, uint32 key1=2, uint64 key2=3)", *key);
  }
}

// Test encoding random strings and ensure that the decoded string
// matches the input.
TEST_F(EncodedKeyTest, TestRandomStringEncoding) {
  Random r(SeedRandom());
  char buf[80];
  faststring encoded;
  for (int i = 0; i < 10000; i++) {
    encoded.clear();
    arena_.Reset();

    int len = r.Uniform(sizeof(buf));
    RandomString(buf, len, &r);

    Slice in_slice(buf, len);
    KeyEncoderTraits<BINARY, faststring>::EncodeWithSeparators(&in_slice, false, &encoded);

    Slice to_decode(encoded);
    Slice decoded_slice;
    // C++ does not allow commas in macro invocations without being wrapped in parenthesis.
    ASSERT_OK((KeyEncoderTraits<BINARY, string>::DecodeKeyPortion(
        &to_decode, false, &arena_, reinterpret_cast<uint8_t*>(&decoded_slice))));

    ASSERT_EQ(decoded_slice.ToDebugString(), in_slice.ToDebugString())
        << "encoded: " << Slice(encoded).ToDebugString();
  }
}

#ifdef NDEBUG

// Without this wrapper function, small changes to the code size of
// EncodeWithSeparators would cause the benchmark to either inline or not
// inline the function under test, making the benchmark unreliable.
ATTRIBUTE_NOINLINE
static void NoInlineDoEncode(Slice s, bool is_last, faststring* dst)  {
  KeyEncoderTraits<BINARY, faststring>::EncodeWithSeparators(s, is_last, dst);
}

TEST_F(EncodedKeyTest, BenchmarkStringEncoding) {
  string data;
  for (int i = 0; i < 100; i++) {
    data += "abcdefghijklmnopqrstuvwxyz";
  }

  for (int size = 0; size < 32; size++) {
    LOG_TIMING(INFO, strings::Substitute("1M strings: size=$0", size)) {
      faststring dst;
      for (int i = 0; i < 1000000; i++) {
        dst.clear();
        NoInlineDoEncode(Slice(data.c_str(), size), false, &dst);
      }
    }
  }
}
#endif
} // namespace kudu
