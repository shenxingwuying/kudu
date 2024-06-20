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
#include <memory>
#include <string>
#include <vector>

#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/bitset.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class ClientServerMapping;
class ColumnSchema;
class KuduPartialRow;
class Schema;

typedef FixedBitSet<RowOperationsPB::Type, RowOperationsPB_Type_Type_ARRAYSIZE> RowOpTypes;

class RowOperationsPBEncoder {
 public:
  explicit RowOperationsPBEncoder(RowOperationsPB* pb);
  ~RowOperationsPBEncoder();

  // Whether there is no row operations in the encoded protobuf message.
  bool empty() const {
    return pb_->mutable_rows()->empty();
  }

  // Append this partial row to the protobuf. Returns the size delta for the
  // underlying protobuf after adding the partial row.
  size_t Add(RowOperationsPB::Type type, const KuduPartialRow& partial_row);

  // Remove the last added row from the underlying protobuf. Calling this method
  // more than one time in a row or when no rows were added triggers
  // CHECK()/abort.
  void RemoveLast();

 private:
  // Get the size estimation (upper boundary) for encoded RowOperationsPB::rows
  // after adding the extra row specified.
  size_t GetRowsFieldSizeEstimate(const KuduPartialRow& partial_row,
                                  size_t* isset_bitmap_size,
                                  size_t* isnon_null_bitmap_size) const;

  RowOperationsPB* pb_;
  std::string::size_type prev_indirect_data_size_;
  std::string::size_type prev_rows_size_;

  DISALLOW_COPY_AND_ASSIGN(RowOperationsPBEncoder);
};

struct DecodedRowOperation {
  RowOperationsPB::Type type;

  // For INSERT, INSERT_IGNORE, UPSERT, or UPSERT_IGNORE, the whole projected row.
  // For UPDATE, UPDATE_IGNORE, DELETE, or DELETE_IGNORE, the row key.
  const uint8_t* row_data;

  // For INSERT or UPDATE, a bitmap indicating which of the cells were
  // explicitly set by the client, versus being filled-in defaults.
  // A set bit indicates that the client explicitly set the cell.
  const uint8_t* isset_bitmap;

  // For UPDATE and DELETE types, the changelist
  RowChangeList changelist;

  // For SPLIT_ROW, the partial row to split on.
  std::shared_ptr<KuduPartialRow> split_row;

  // Per-row result status.
  Status result;

  // True if an ignore op was ignored due to an error.
  // As of now, the error could be one of the following:
  // - UPDATE_IGNORE op on a row to update an immutable column.
  bool error_ignored = false;

  // Stringifies, including redaction when appropriate.
  std::string ToString(const Schema& schema) const;

  // The 'result' member will only be updated the first time this function is called.
  void SetFailureStatusOnce(Status s);
};

enum DecoderMode {
  // Decode range split rows.
  SPLIT_ROWS,

  // Decode write operations.
  WRITE_OPS,
};

class RowOperationsPBDecoder {
 public:
  RowOperationsPBDecoder(const RowOperationsPB* pb,
                         const Schema* client_schema,
                         const Schema* tablet_schema,
                         Arena* dst_arena);
  ~RowOperationsPBDecoder();

  template <DecoderMode mode>
  Status DecodeOperations(std::vector<DecodedRowOperation>* ops);

 private:
  Status ReadOpType(RowOperationsPB::Type* type);
  Status ReadIssetBitmap(const uint8_t** bitmap);
  Status ReadNullBitmap(const uint8_t** null_bm);
  // Read one row's column data from 'src_', read result is stored in 'slice'.
  // Return bad Status if data is corrupt.
  // NOTE: If 'row_status' is not nullptr, column data validate will be performed,
  // and if column data validate error (i.e. column size exceed the limit), only
  // set bad Status to 'row_status', and return Status::OK.
  Status GetColumnSlice(const ColumnSchema& col, Slice* slice, Status* row_status);
  // Same as above, but store result in 'dst'.
  Status ReadColumn(const ColumnSchema& col, uint8_t* dst, Status* row_status);
  // Some column which is non-nullable has allocated a cell to row data in
  // RowOperationsPBEncoder::Add, even if its data is useless (i.e. set to
  // NULL), we have to consume data in order to properly validate subsequent
  // columns and rows.
  Status ReadColumnAndDiscard(const ColumnSchema& col);
  bool HasNext() const;
  // Returns 'client_col_idx' if client and tablet schemas match, if not uses
  // 'mapping' to translate client schema idx to tablet schema idx.
  size_t GetTabletColIdx(const ClientServerMapping& mapping, size_t client_col_idx);

  Status DecodeInsertOrUpsert(const uint8_t* prototype_row_storage,
                              const ClientServerMapping& mapping,
                              DecodedRowOperation* op);
  //------------------------------------------------------------
  // Serialization/deserialization support
  //------------------------------------------------------------

  // Decode the next encoded operation, which must be UPDATE or DELETE.
  Status DecodeUpdateOrDelete(const ClientServerMapping& mapping,
                              DecodedRowOperation* op);

  // Decode the next encoded operation, which must be SPLIT_KEY.
  Status DecodeSplitRow(const ClientServerMapping& mapping,
                        DecodedRowOperation* op);

  // Decode the next encoded operation of the given type and properties.
  // Returns an error if the type isn't allowed by the decoder mode.
  template <DecoderMode mode>
  Status DecodeOp(RowOperationsPB::Type type, const uint8_t* prototype_row_storage,
                  const ClientServerMapping& mapping, DecodedRowOperation* op);

  const RowOperationsPB* const pb_;
  // If 'client_schema_' and 'tablet_schema_' are the same object, the mapping
  // between the two schemas is effectively useless since only one schema
  // exists. This is the only time when the client_schema_ can have column IDs.
  const Schema* const client_schema_;
  const Schema* const tablet_schema_;
  Arena* const dst_arena_;

  const int bm_size_;
  const size_t tablet_row_size_;
  Slice src_;

  DISALLOW_COPY_AND_ASSIGN(RowOperationsPBDecoder);
};

} // namespace kudu
