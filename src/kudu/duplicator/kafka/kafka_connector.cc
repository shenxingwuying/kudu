#include "kudu/duplicator/kafka/kafka_connector.h"

#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/util/slice.h"

using kudu::kafka::RawKuduRecord;
using std::string;

DEFINE_int32(kafka_retry_num, 3, "kafka api retry num if failed");

// support kerberos, detail in https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka,
// the page's the 5th part: 5.Configure Kafka client on client host
DEFINE_string(security_protocol, "SASL_PLAINTEXT", "security protocol"
              ", such as PLAINTEXT,SSL,SASL_PLAINTEXT,SASL_SSL");
DEFINE_string(keytab_full_path, "", "keytab file's full path");
DEFINE_string(principal_name, "", "principal info, your should input correct info");

DEFINE_int32(min_topic_partition_num, 1, "the min value of topic's partition num");

DECLARE_string(kafka_broker_list);
DECLARE_string(kafka_target_topic);

namespace kudu {
namespace kafka {

Status ParseRow(const DecodedRowOperation& decoded_row,
                const Schema* schema,
                RawKuduRecord* insert_value_pb,
                uint64_t* primary_key_hash_code) {
  ConstContiguousRow parsed_row = ConstContiguousRow(schema, decoded_row.row_data);

  RangePKToPB(*schema, parsed_row, insert_value_pb, primary_key_hash_code);

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
  return insert_value_pb->properties().empty() ?
             Status::Corruption("range to pb error") : Status::OK();
}

Status ChangesToColumn(const RowChangeList& changelist,
                       const Schema* schema,
                       RawKuduRecord* update_value_pb) {
  DCHECK_GT(changelist.slice().size(), 0);
  RowChangeListDecoder decoder(changelist);
  Status s = decoder.Init();

  while (decoder.HasNext()) {
    RowChangeListDecoder::DecodedUpdate dec;
    int col_idx;
    const void* value;
    s = decoder.DecodeNext(&dec);
    if (s.ok()) {
      s = dec.Validate(*schema, &col_idx, &value);
    } else {
      // invalid record
      return update_value_pb->properties().empty() ?
              Status::Corruption("invalid record") : Status::OK();
    }
    // Known column.
    const ColumnSchema& col_schema = schema->column(col_idx);
    RawProperty* property = update_value_pb->add_properties();
    property->set_primary_key_column(false);
    property->set_name(col_schema.name());

    if (value != nullptr) {
      string ret;
      col_schema.type_info()->StringForValue(value, &ret);
      property->set_null_value(false);
      property->set_value(ret);
    } else {
      property->set_value("");
      property->set_null_value(true);
    }
  }

  return update_value_pb->properties().empty() ?
            Status::Corruption("range to pb error") : Status::OK();
}

}  // namespace kafka
}  // namespace kudu
