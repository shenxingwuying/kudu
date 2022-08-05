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

#include "kudu/util/seda.h"

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

namespace kudu {
namespace seda {

struct SimpleArithmeticContext : AsyncContext {
  int result;
  int retry;
  int stop_num;
  int expect;
  SimpleArithmeticContext(Action action, std::shared_ptr<AsyncClient> client)
      : AsyncContext(action, std::move(client)) {}
};

class SimpleAsyncClientTest : public AsyncClient {
 public:
  explicit SimpleAsyncClientTest(ThreadPoolToken* token) : AsyncClient(token) {}
  ~SimpleAsyncClientTest() override {}

  void OnStagedEventDriven(AsyncContext* context) override {
    VLOG(1) << "received action: " << Action2String(context->action);
    switch (context->action) {
      case Action::kTestAdd:
        OnAdd(context);
        break;
      case Action::kTestStart:
        OnStart(context);
        break;
      case Action::kTestStop:
        OnStop(context);
        break;
      default:
        VLOG(0) << "unsupported action: " << Action2String(context->action);
        break;
    }
  }
  void OnAdd(AsyncContext* context) {
    std::unique_ptr<AsyncContext> context_ptr(context);
    SimpleArithmeticContext* ctx = reinterpret_cast<SimpleArithmeticContext*>(context);
    if (++ctx->retry <= ctx->stop_num) {
      (void) context_ptr.release();
      ctx->result++;
      VLOG(0) << "result: " << ctx->result;
      threadpool_token_->Execute(context);
    } else {
      ASSERT_EQ(ctx->expect, ctx->result);
    }
  }
  void OnStart(AsyncContext* /* context */) {}
  void OnStop(AsyncContext* /* context */) {}
};

class SEDATest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(ThreadPoolBuilder("test").Build(&pool_));
    token_ = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
    client_ = std::make_shared<SimpleAsyncClientTest>(token_.get());
  }
  void TearDown() override {
    KuduTest::TearDown();
  }

 protected:
  std::unique_ptr<ThreadPool> pool_;
  std::unique_ptr<ThreadPoolToken> token_;
  std::shared_ptr<SimpleAsyncClientTest> client_;
};

TEST_F(SEDATest, SimpleSedaOK) {
  std::unique_ptr<ThreadPoolToken> token_ptr = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
  ThreadPoolToken* token = token_ptr.get();
  std::shared_ptr<SimpleAsyncClientTest> client = std::make_shared<SimpleAsyncClientTest>(token);
  client->InitWeakPtr(client);
  SimpleArithmeticContext* context =
      new SimpleArithmeticContext(Action::kTestAdd, client->GetSharedPtr());
  context->result = 0;
  context->retry = 0;
  context->stop_num = 5;
  context->expect = 5;
  token->Execute(context);
  token->Wait();
}

TEST_F(SEDATest, SimpleSedaAnotherOK) {
  SimpleArithmeticContext* context =
      new SimpleArithmeticContext(Action::kTestAdd, client_);
  context->result = 0;
  context->retry = 0;
  context->stop_num = 5;
  context->expect = 5;
  token_->Execute(context);
  token_->Wait();
}

}  // namespace seda
}  // namespace kudu
