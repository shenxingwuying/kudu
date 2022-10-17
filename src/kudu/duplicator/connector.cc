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

#include "kudu/duplicator/connector.h"

#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/duplicator/kafka/kafka_connector.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace kafka {

Status ParseRow(const DecodedRowOperation& decoded_row,
                const Schema* schema,
                RawKuduRecord* insert_value_pb,
                std::string* primary_key) {
  ConstContiguousRow parsed_row = ConstContiguousRow(schema, decoded_row.row_data);
  RangePKToPB(*schema, parsed_row, insert_value_pb, primary_key);

  if (decoded_row.type == RowOperationsPB::DELETE ||
      decoded_row.type == RowOperationsPB::DELETE_IGNORE) {
    // When deleting, only key is needed;
    return Status::OK();
  }
  if (decoded_row.type == RowOperationsPB::UPDATE ||
      decoded_row.type == RowOperationsPB::UPDATE_IGNORE) {
    // When updating, the value's data is stored in changelist instead.
    return ChangesToColumn(decoded_row.changelist, schema, insert_value_pb);
  }
  // The operations below are insert/upsert.
  RangeToPB(*schema, parsed_row, decoded_row.isset_bitmap, insert_value_pb);
  return insert_value_pb->properties().empty() ? Status::Corruption("range to pb error")
                                               : Status::OK();
}

Status ChangesToColumn(const RowChangeList& changelist,
                       const Schema* schema,
                       RawKuduRecord* update_value_pb) {
  DCHECK(!changelist.slice().empty());
  RowChangeListDecoder decoder(changelist);
  RETURN_NOT_OK(decoder.Init());
  while (decoder.HasNext()) {
    RowChangeListDecoder::DecodedUpdate dec;
    int col_idx;
    const void* value;
    RETURN_NOT_OK(decoder.DecodeNext(&dec));
    RETURN_NOT_OK(dec.Validate(*schema, &col_idx, &value));
    // Known column.
    const ColumnSchema& col_schema = schema->column(col_idx);
    RawProperty* property = update_value_pb->add_properties();
    property->set_primary_key_column(false);
    property->set_name(col_schema.name());

    if (value != nullptr) {
      std::string ret;
      col_schema.type_info()->StringForValue(value, &ret);
      property->set_null_value(false);
      property->set_value(ret);
    } else {
      property->set_value("");
      property->set_null_value(true);
    }
  }

  return update_value_pb->properties().empty() ? Status::Corruption("range to pb error")
                                               : Status::OK();
}

}  // namespace kafka
}  // namespace kudu
