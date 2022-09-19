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
// the codes based on some ningw's internal work, and then refact.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/duplicator/connector.h"
#include "kudu/duplicator/kafka/kafka.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/metrics.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace cppkafka {
class Message;
class Producer;
}  // namespace cppkafka

namespace kudu {
namespace kafka {

template <class RowType>
void RangeToPB(const Schema& schema,
               const RowType& row,
               const uint8_t* isset_bitmap,
               RawKuduRecord* insert_value_pb) {
  size_t low = schema.num_key_columns();
  size_t high = schema.num_columns();
  for (size_t i = low; i < high; ++i) {

    if (!BitmapTest(isset_bitmap, i)) {
      continue;
    }
    const ColumnSchema& col = schema.column(i);
    auto* property = insert_value_pb->add_properties();
    if (col.is_nullable() && row.cell(i).is_null()) {
      property->set_value("");
      property->set_null_value(true);
    } else {
      property->set_null_value(false);
      std::string ret;
      col.type_info()->StringForValue(row.cell(i).ptr(), &ret);
      property->set_value(ret);
    }
    property->set_name(col.name());
    property->set_primary_key_column(false);
  }
}

template <class RowType>
void RangePKToPB(const Schema& schema,
                 const RowType& row,
                 kudu::kafka::RawKuduRecord* insert_value_pb,
                 std::string* primary_key) {
  size_t pk_num = schema.num_key_columns();
  for (size_t i = 0; i < pk_num; ++i) {
    const ColumnSchema& col = schema.column(i);
    std::string ret;
    col.type_info()->StringForValue(row.cell(i).ptr(), &ret);
    auto* property = insert_value_pb->add_properties();
    property->set_name(col.name());
    property->set_value(ret);
    property->set_primary_key_column(true);
    property->set_null_value(false);
    primary_key->append(ret);
  }
}

class KafkaConnector : public duplicator::Connector {
 public:
  explicit KafkaConnector(duplicator::ConnectorOptions options);
  ~KafkaConnector() override;

  Status Init(const duplicator::ConnectorOptions& options) override;

  // consume content in kafka_record_queue and flush to kafka
  Status WriteBatch(
      const std::string& topic_name,
      const std::vector<std::unique_ptr<duplicator::DuplicateMsg>>& messages) override;

  Status TestBasicClientApi(const std::string& topic_name, const std::string& msg) override;

  std::shared_ptr<cppkafka::Producer> producer() {
    return producer_;
  }

  consensus::DownstreamType Type() const override {
    return consensus::DownstreamType::KAFKA;
  }

  std::string Uri() const override {
    return brokers_;
  }

  duplicator::ConnectorOptions connector_options() const override {
    return options_;
  }

 private:
  Status Flush();

  void DeliveryReport(cppkafka::Producer& /*producer*/, const cppkafka::Message& message); // NOLINT

  const duplicator::ConnectorOptions options_;

  // kafka brokers for kafka uri.
  const std::string brokers_;

  // kafka producer, would write record to kafka.
  std::shared_ptr<cppkafka::Producer> producer_;

  // To make producer_ service only a duplicator at a time,
  // The 'mutex' make 'WriteBatch'(produce() and flush()) as a atomic operation.
  mutable Mutex mutex_;

  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<Counter> duplication_ops_;
  scoped_refptr<Histogram> kafka_flush_histogram_;
};

}  // namespace kafka
}  // namespace kudu
