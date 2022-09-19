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
// The file is define a Common Connector interface api,
// if user need to duplicate data to another storage,
// user should extend the interface just as kafka/kafka_connector.h.

#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/status.h"

namespace kudu {
class RowChangeList;
class Schema;

namespace kafka {
extern Status ParseRow(const DecodedRowOperation& decoded_row,
                       const Schema* schema,
                       RawKuduRecord* insert_value_pb,
                       std::string* primary_key);
extern Status ChangesToColumn(const RowChangeList& changelist,
                              const Schema* schema,
                              RawKuduRecord* update_value_pb);
}  // namespace kafka

namespace duplicator {

using std::string;

// A simple pair: Primary Key -> RawKuduRecord, avoid deep copy
struct KafkaMessage {
  std::string primary_key;
  kudu::kafka::RawKuduRecord record;
  KafkaMessage(std::string&& p_primary_key, kudu::kafka::RawKuduRecord&& p_record)
      : primary_key(std::move(p_primary_key)), record(std::move(p_record)) {}
};

class DuplicateMsg {
 public:
  DuplicateMsg(tablet::WriteOpState* op_state, string table_name)
      : op_state_(op_state), table_name_(std::move(table_name)), op_id_(op_state_->op_id()) {}
  ~DuplicateMsg() {}

  tablet::WriteOpState* op_state() const { return op_state_; }

  consensus::OpId op_id() const { return op_id_; }

  // TODO(duyuqi).
  // We should move the work into in duplicate_pool.
  //
  // The implements is not suitable indeed.
  // This is a temporory method to solve a problem. I try my best to do this in another thread,
  // which parse 'DuplicateMsg''s ops to the conresponding KafkaRecord, but because of
  // op_state's ops, they use Arena store information and variable blobs(such as string) was changed
  // unexpectly, I have to do this in the apply_pool rather than the duplicate_pool.
  Status ParseKafkaRecord() {
    result_.clear();
    result_.reserve(op_state_->row_ops().size());
    const Schema* schema = op_state_->schema_at_decode_time();
    for (const auto& row_op : op_state_->row_ops()) {
      if (row_op->result && row_op->result->has_failed_status()) {
        LOG(INFO) << strings::Substitute("duplication skip a row_op, row_op: $0, failed_status: $1",
                                         row_op->ToString(*schema),
                                         row_op->result->failed_status().ShortDebugString());
        continue;
      }
      kudu::kafka::RawKuduRecord record;
      string primary_key;
      RETURN_NOT_OK(
          kudu::kafka::ParseRow(row_op->decoded_op, schema, &record, &primary_key));
      record.set_table_name(table_name_);
      switch (row_op->decoded_op.type) {
        case RowOperationsPB::INSERT:
        case RowOperationsPB::INSERT_IGNORE:
          record.set_operation_type(kudu::kafka::RawKuduRecord::INSERT);
          break;
        case RowOperationsPB::UPSERT: {
          // TODO(duyuqi)
          // we need split upsert to insert/update because upsert is kudu's sematics,
          // in general not supported in other systems ?
          record.set_operation_type(kudu::kafka::RawKuduRecord::UPSERT);
          break;
        }
        case RowOperationsPB::UPDATE:
        case RowOperationsPB::UPDATE_IGNORE:
          record.set_operation_type(kudu::kafka::RawKuduRecord::UPDATE);
          break;
        case RowOperationsPB::DELETE:
        case RowOperationsPB::DELETE_IGNORE:
          record.set_operation_type(kudu::kafka::RawKuduRecord::DELETE);
          break;
        default:
          LOG(FATAL) << "unknown RowOperationsPB type"
                     << RowOperationsPB::Type_Name(row_op->decoded_op.type);
          break;
      }

      std::shared_ptr<KafkaMessage> kafka_message =
          std::make_shared<KafkaMessage>(std::move(primary_key), std::move(record));
      result_.emplace_back(kafka_message);
    }
    return Status::OK();
  }

  const std::vector<std::shared_ptr<KafkaMessage>>& result() const { return result_; }

 private:
  tablet::WriteOpState* op_state_;
  string table_name_;

  // OpId of op_state_.
  consensus::OpId op_id_;

  // Convert ops of op_state_ into result.
  std::vector<std::shared_ptr<KafkaMessage>> result_;

  DISALLOW_COPY_AND_ASSIGN(DuplicateMsg);
};

struct KerberosOptions {
  string security_protocol;
  string keytab;
  string principal;
};

struct ConnectorOptions {
  // Duplication's destination storage system, eg: kafka, pulsar.
  consensus::DownstreamType type;
  // The entity of destination storage system, eg: kafka's topic name.
  string name;
  // Destination storage system uri, eg: kafka brokers list.
  string uri;
  // Optional infomations, eg: security options.
  string options;

  // Kerberos security options.
  std::optional<kudu::KerberosOptions> kerberos_options;

  ConnectorOptions() {}

  explicit ConnectorOptions(const consensus::DuplicationInfoPB& duplication_info_pb)
      : type(duplication_info_pb.type()),
        name(duplication_info_pb.name()),
        uri(duplication_info_pb.uri()),
        options(duplication_info_pb.options()),
        kerberos_options(
            duplication_info_pb.has_kerberos_options()
                ? std::make_optional<kudu::KerberosOptions>(duplication_info_pb.kerberos_options())
                : std::nullopt) {}

  string ToString() const {
    string result = strings::Substitute("type: $0, name: $1, uri: $2, options: $3",
                                        consensus::DownstreamType_Name(type),
                                        name, uri, options);
    if (kerberos_options) {
      return strings::Substitute(
          "$0, security_protocol: $1, service_name: $2, "
          "keytab: $3, principal: $4",
          result,
          kudu::KerberosOptions::SecurityProtocol_Name(kerberos_options->security_protocol()),
          kerberos_options->service_name(),
          kerberos_options->keytab(),
          kerberos_options->principal());
    }
    return result;
  }

  bool IsKerberosOptionsEqual(const ConnectorOptions& options) const {
    if (kerberos_options.has_value() && options.kerberos_options.has_value()) {
      return kerberos_options->security_protocol() ==
                 options.kerberos_options->security_protocol() &&
             kerberos_options->service_name() == options.kerberos_options->service_name() &&
             kerberos_options->keytab() == options.kerberos_options->keytab() &&
             kerberos_options->principal() == options.kerberos_options->principal();
    }
    return kerberos_options.has_value() == options.kerberos_options.has_value();
  }
};

class Connector {
 public:
  virtual ~Connector() = default;
  virtual Status Init(const ConnectorOptions& options) = 0;
  virtual Status WriteBatch(const std::string& entity_name,
                            const std::vector<std::unique_ptr<DuplicateMsg>>& msgs) = 0;
  // For test basic client api
  virtual Status TestBasicClientApi(const std::string& /*entity_name*/,
                                    const std::string& /*msg*/) {
    return Status::OK();
  }
  virtual consensus::DownstreamType Type() const = 0;
  virtual std::string Uri() const = 0;
  virtual ConnectorOptions connector_options() const = 0;
};

}  // namespace duplicator
}  // namespace kudu
