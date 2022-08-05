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

#include <memory>
#include <utility>

namespace kudu {
class ThreadPoolToken;

namespace seda {

// Define actions of stage tasks.
enum class Action {
  // For Test actions.
  kTestStart = 0,
  kTestStop,
  kTestAdd,

  // For Master actions.
  kMasterLeaderRebalance,
  kMasterReplicaRebalance,

  // For TServer actions.
  kTServerFollowerWaitIndex,
  kTServerNewScan,
  kTServerContinueScan
  // For Tools actions.
};

// Get human readable info of 'Action'.
extern const char* Action2String(Action action);

class AsyncClient;

// Context save infomation of stage tasks.
// User-define Context should extends 'AsyncContext'.
// Uasage:
//   struct SubAsyncContext : AsyncContext {
//       ...
//   }
//
// More details, see seda-test.cc example.
struct AsyncContext {
  Action action;
  std::shared_ptr<AsyncClient> client;

  AsyncContext(Action action, std::shared_ptr<AsyncClient> client)
      : action(action), client(std::move(client)) {}
};

// Usage:
//   class SubAsyncClient : public AsyncClient {
//     public:
//       void OnStagedEventDriven(AsyncContext* ctx) override {
//         switch(ctx->action) {
//           case Action::kTestStart:
//             OnStart(ctx);
//             break;
//           ...
//           default:
//             break;
//         }
//         ...
//       }
//     private:
//       void OnStart(AsyncContext* context) { ... }
//   }
//
//   // AsyncContext* ctx;
//   // AsyncClient* client;
//   client->thread_pool_token()->Execute(ctx);
//
// The statement would execute the user-defined the action of AsyncClient's subclass.
// More details, see seda-test.cc example.
class AsyncClient {
 public:
  explicit AsyncClient(ThreadPoolToken* threadpool_token = nullptr)
      : threadpool_token_(threadpool_token) {}
  virtual ~AsyncClient() {}

  virtual void InitWeakPtr(const std::shared_ptr<AsyncClient>& client) { weak_ptr_ = client; }
  std::shared_ptr<AsyncClient> GetSharedPtr() { return weak_ptr_.lock(); }

  void set_threadpool_token(ThreadPoolToken* threadpool_token) {
    threadpool_token_ = threadpool_token;
  }

  ThreadPoolToken* thread_pool_token() { return threadpool_token_; }
  virtual void OnStagedEventDriven(AsyncContext* context) = 0;

 protected:
  ThreadPoolToken* threadpool_token_;

 private:
  // Get shared_ptr object and avoid circular reference.
  std::weak_ptr<AsyncClient> weak_ptr_;
};

}  // namespace seda
}  // namespace kudu
