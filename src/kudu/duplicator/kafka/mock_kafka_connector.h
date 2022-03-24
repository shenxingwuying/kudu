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
// The file is for mock unit test.

#include "cppkafka/cppkafka.h"

DECLARE_string(kafka_broker_list);
DECLARE_string(kafka_target_topic);
DECLARE_int32(kafka_retry_num);
DECLARE_string(downstream_type);

namespace cppkafka {

class MockProducer {
 public:
  explicit MockProducer(Configuration config) : timeout_ms_(30000) {}

  Metadata get_metadata() {
    Metadata meta;
    return meta;
  }
  int timeout_ms() { return timeout_ms_; }

  // cppkafka::MessageBuilder
  void produce(const MessageBuilder& message) { produce(message, false); }
  void produce(const MessageBuilder& /* message */, bool fail_injection) {
    rd_kafka_resp_err_t error = RD_KAFKA_RESP_ERR_NO_ERROR;
    if (fail_injection) {
      error = RD_KAFKA_RESP_ERR_INVALID_MSG;
    }
    check_error(error);
  }

  void flush() { flush(std::chrono::milliseconds(timeout_ms_), false); }
  void flush(bool fail_injection) { flush(std::chrono::milliseconds(timeout_ms_), fail_injection); }

  /**
   * \brief Mock Flush all outstanding produce requests
   * \param timeout The timeout used on this call
   */
  void flush(std::chrono::milliseconds timeout) { flush(timeout, false); }
  void flush(std::chrono::milliseconds timeout, bool fail_injection) {
    rd_kafka_resp_err_t error = RD_KAFKA_RESP_ERR_NO_ERROR;
    if (fail_injection) {
      error = RD_KAFKA_RESP_ERR_INVALID_MSG;
    }
    check_error(error);
  }
  void check_error(rd_kafka_resp_err_t error) {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw HandleException(error);
    }
  }

 private:
  int timeout_ms_;
};
}  // namespace cppkafka
