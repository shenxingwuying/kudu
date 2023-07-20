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

#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/echo_subprocess.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(subprocess_request_queue_size_bytes);
DECLARE_int32(subprocess_response_queue_size_bytes);
DECLARE_int32(subprocess_num_responder_threads);
DECLARE_int32(subprocess_timeout_secs);
DECLARE_uint32(subprocess_max_message_size_bytes);

using google::protobuf::Any;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace subprocess {

namespace {

// Creates a subprocess request with the given payload and ms to sleep.
SubprocessRequestPB CreateEchoSubprocessRequestPB(const string& payload,
                                                  int sleep_ms = 0) {
  SubprocessRequestPB request;
  EchoRequestPB echo_request;
  echo_request.set_data(payload);
  echo_request.set_sleep_ms(sleep_ms);
  unique_ptr<Any> any(new Any);
  any->PackFrom(echo_request);
  request.set_allocated_request(any.release());
  return request;
}

Status CheckMessage(const SubprocessResponsePB& resp, const string& expected_msg) {
  EchoResponsePB echo_resp;
  if (!resp.response().UnpackTo(&echo_resp)) {
    return Status::Corruption(Substitute("Failed to unpack echo response: $0",
                                         pb_util::SecureDebugString(resp)));
  }
  if (expected_msg != echo_resp.data()) {
    return Status::Corruption(Substitute("Expected: '$0', got: '$1'",
                                         expected_msg, echo_resp.data()));
  }
  return Status::OK();
}

const char* kHello = "hello world";

} // anonymous namespace

class SubprocessServerTest : public KuduTest {
 public:
  SubprocessServerTest()
      : test_dir_(GetTestDataDirectory()),
        metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                        "subprocess_server-test")) {}
  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(ResetSubprocessServer());
  }

  Status InitSubprocessServer(int java_queue_size,
                              int java_parser_threads,
                              int java_max_msg_bytes,
                              shared_ptr<SubprocessServer>* out) {
    // Set up a subprocess server pointing at the kudu-subprocess.jar that
    // contains an echo handler and call EchoSubprocessMain.
    string exe;
    RETURN_NOT_OK(env_->GetExecutablePath(&exe));
    const string bin_dir = DirName(exe);
    string java_home;
    RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));
    const string pipe_path = SubprocessServer::FifoPath(JoinPathSegments(test_dir_, "echo_pipe"));
    vector<string> argv = {
      Substitute("$0/bin/java", java_home),
      "-cp", Substitute("$0/kudu-subprocess.jar", bin_dir),
      "org.apache.kudu.subprocess.echo.EchoSubprocessMain",
      "-o", pipe_path,
    };
    if (java_queue_size > 0) {
      argv.emplace_back("-q");
      argv.emplace_back(std::to_string(java_queue_size));
    }
    if (java_parser_threads > 0) {
      argv.emplace_back("-p");
      argv.emplace_back(std::to_string(java_parser_threads));
    }
    if (java_max_msg_bytes > 0) {
      argv.emplace_back("-m");
      argv.emplace_back(std::to_string(java_max_msg_bytes));
    }
    *out = make_shared<SubprocessServer>(env_, pipe_path, std::move(argv),
                                         EchoSubprocessMetrics(metric_entity_));
    return (*out)->Init();
  }

  // Resets the subprocess server to account for any new configuration.
  Status ResetSubprocessServer(int java_queue_size = 0,
                               int java_parser_threads = 0,
                               int java_max_msg_bytes = 0) {
    return InitSubprocessServer(java_queue_size, java_parser_threads, java_max_msg_bytes, &server_);
  }

 protected:
  const string test_dir_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  shared_ptr<SubprocessServer> server_;
};

TEST_F(SubprocessServerTest, TestBasicCall) {
  SubprocessRequestPB request = CreateEchoSubprocessRequestPB(kHello);
  SubprocessResponsePB response;
  ASSERT_OK(server_->Execute(&request, &response));
  EchoResponsePB echo_response;
  ASSERT_TRUE(response.response().UnpackTo(&echo_response));
  ASSERT_EQ(echo_response.data(), kHello);
}

// Test sending many requests concurrently.
TEST_F(SubprocessServerTest, TestManyConcurrentCalls) {
  constexpr int kNumThreads = 20;
  constexpr int kNumPerThread = 200;
  const string kEchoDataPrefix = "Do the hokey pokey, turn yourself around!";
  vector<vector<SubprocessRequestPB>> requests(kNumThreads,
      vector<SubprocessRequestPB>(kNumPerThread));
  vector<vector<SubprocessResponsePB>> responses(kNumThreads,
      vector<SubprocessResponsePB>(kNumPerThread));
  for (int t = 0; t < kNumThreads; t++) {
    for (int i = 0; i < kNumPerThread; i++) {
      requests[t][i] = CreateEchoSubprocessRequestPB(
          Substitute("$0: thread $1 idx $2", kEchoDataPrefix, t, i));
    }
  }
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  {
    vector<thread> threads;
    SCOPED_CLEANUP({
      for (auto& t : threads) {
        t.join();
      }
    });
    for (int t = 0; t < kNumThreads; t++) {
      threads.emplace_back([&, t] {
        for (int i = 0; i < kNumPerThread; i++) {
          ASSERT_OK(server_->Execute(&requests[t][i], &responses[t][i]));
        }
      });
    }
  }
  sw.stop();
  double reqs_sent = kNumThreads * kNumPerThread;
  double elapsed_seconds = sw.elapsed().wall_seconds();
  LOG(INFO) << Substitute("Sent $0 requests in $1 seconds: $2 req/s",
                          reqs_sent, elapsed_seconds, reqs_sent / elapsed_seconds);
  for (int t = 0; t < kNumThreads; t++) {
    for (int i = 0; i < kNumPerThread; i++) {
      EchoRequestPB echo_req;
      requests[t][i].request().UnpackTo(&echo_req);
      ASSERT_OK(CheckMessage(responses[t][i], echo_req.data()));
    }
  }
}

// Test when our timeout occurs before adding the call to the outbound queue.
TEST_F(SubprocessServerTest, TestTimeoutBeforeQueueing) {
  FLAGS_subprocess_timeout_secs = 0;
  ASSERT_OK(ResetSubprocessServer());

  SubprocessRequestPB request = CreateEchoSubprocessRequestPB(kHello);
  SubprocessResponsePB response;
  Status s = server_->Execute(&request, &response);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "couldn't enqueue call");
}

// Test when we try sending too many requests at once.
TEST_F(SubprocessServerTest, TestTimeoutWhileQueueingCalls) {
  // Set a relatively low timeout so our calls timeout more easily.
  FLAGS_subprocess_timeout_secs = 1;

  // Set a really low queue size so we can overflow the outbound queue easily.
  FLAGS_subprocess_request_queue_size_bytes = 1;

  // Make the Java subprocess single-threaded so we can overwhelm it more
  // easily.
  ASSERT_OK(ResetSubprocessServer(/*java_queue_size*/1,
                                  /*java_parser_threads*/1));

  // Send a bunch of requests with a sleep that's lower than our timeout.
  // Since we've made the subprocess single-threaded, the sheer number of
  // requests should fill the pipe, and our outbound queue.
  const int kNumRequests = 500;
  vector<thread> threads;
  threads.reserve(kNumRequests);
  vector<Status> results(kNumRequests);
  const string kLargeRequest = string(10000, 'x');
  for (int i = 0; i < kNumRequests; i++) {
    threads.emplace_back([&, i] {
      SubprocessRequestPB request =
          CreateEchoSubprocessRequestPB(kLargeRequest, /*sleep_ms*/900);
      SubprocessResponsePB response;
      results[i] = server_->Execute(&request, &response);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  bool has_timeout_when_queueing = false;
  for (const auto& s : results) {
    if (s.IsTimedOut() &&
        s.ToString().find("couldn't enqueue call") != string::npos) {
      has_timeout_when_queueing = true;
    }
  }
  // We sent a ton of requests and should've overwhelmed the pipe and our
  // outbound queue.
  ASSERT_TRUE(has_timeout_when_queueing) << "expected at least one timeout";
}

// Test when the subprocess takes too long.
TEST_F(SubprocessServerTest, TestSlowSubprocessTimesOut) {
  FLAGS_subprocess_timeout_secs = 1;
  ASSERT_OK(ResetSubprocessServer());
  SubprocessRequestPB request =
      CreateEchoSubprocessRequestPB(kHello, /*sleep_ms*/1500);
  SubprocessResponsePB response;
  Status s = server_->Execute(&request, &response);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "timed out while in flight");
}

// Test calls while shutting down.
TEST_F(SubprocessServerTest, TestCallsReturnWhenShuttingDown) {
  Status s;
  thread t([&] {
    SubprocessRequestPB request =
        CreateEchoSubprocessRequestPB(kHello, /*sleep_ms*/1000);
    SubprocessResponsePB response;
    s = server_->Execute(&request, &response);
  });
  server_->Shutdown();
  t.join();
  // There are many places the error could've happened, so we won't check on
  // the exact message or error type.
  ASSERT_FALSE(s.ok());
}

// Some usage of a subprocess warrants calling Init() from a short-lived
// thread. Let's ensure there's no funny business when that happens (e.g.
// ensure the OS doesn't reap the underlying process when the parent thread
// exits).
TEST_F(SubprocessServerTest, TestInitFromThread) {
  Status s;
  thread t([&] {
    s = ResetSubprocessServer();
  });
  t.join();
  ASSERT_OK(s);
  // Wait a bit to give time for the OS to wreak havoc (though it shouldn't).
  SleepFor(MonoDelta::FromSeconds(3));
  SubprocessRequestPB request = CreateEchoSubprocessRequestPB(kHello);
  SubprocessResponsePB response;
  ASSERT_OK(server_->Execute(&request, &response));
}

// Test that we've configured out subprocess server such that we can run it
// from multiple threads without having them collide with each other.
TEST_F(SubprocessServerTest, TestRunFromMultipleThreads) {
  const int kNumThreads = 3;
  vector<thread> threads;
  vector<Status> results(kNumThreads);
#define EXIT_NOT_OK(s, n) do { \
  Status _s = (s); \
  if (!_s.ok()) { \
    results[n] = _s; \
    return; \
  } \
} while (0);

  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      shared_ptr<SubprocessServer> server;
      EXIT_NOT_OK(InitSubprocessServer(0, 0, 0, &server), i);
      const string msg = Substitute("$0 bottles of tea on the wall", i);
      SubprocessRequestPB req = CreateEchoSubprocessRequestPB(msg);
      SubprocessResponsePB resp;
      EXIT_NOT_OK(server->Execute(&req, &resp), i);
      EXIT_NOT_OK(CheckMessage(resp, msg), i);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& r : results) {
    ASSERT_OK(r);
  }
}

// The subprocess server should reject messages in protobuf format if their
// payload is over the threshold specified by the
// --subprocess_max_message_size_bytes flag.
TEST_F(SubprocessServerTest, MaxPayloadSize) {
  // Set a short timeout to speed up testing.
  FLAGS_subprocess_timeout_secs = 1;

  // The protobuf-encoded message has some metadata, so for a single-character
  // payload let's set the upper limit to 100 to make sure the encoded message
  // size is still under 100 bytes.
  FLAGS_subprocess_max_message_size_bytes = 100;
  ASSERT_OK(ResetSubprocessServer());

  // Send in a message that isn't oversized as per the current limit.
  {
    auto req = CreateEchoSubprocessRequestPB("0");
    SubprocessResponsePB res;
    ASSERT_OK(server_->Execute(&req, &res));
  }

  // Send an oversized message.
  {
    auto req = CreateEchoSubprocessRequestPB(string(100, 'x'));
    SubprocessResponsePB res;
    const auto s = server_->Execute(&req, &res);

    // The request will timeout because the oversized response is read and
    // discarded, and there isn't any application-level data to be sent back.
    // Unfortunately, the current design of the subprocess protocol doesn't
    // allow for reporting on communication errors between the server and
    // the subprocesses it spawned.
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "timed out while in flight");
  }

  // Non-oversized follow-up messages should be received without any issues:
  // the communication channel should be cleared of any oversized requests
  // sent earlier.
  {
    auto req = CreateEchoSubprocessRequestPB("1");
    SubprocessResponsePB res;
    ASSERT_OK(server_->Execute(&req, &res));
  }
}

// Test the semantics of the --subprocess_max_message_size_bytes=0 setting,
// meaning there isn't upper limit on the size of protobuf-encoded responses
// from the subprocess.
//
// NOTE: the Java-based implementation of the related Subprocess components
//       and test harnesses have their own limit on the maximum allowed size
//       of the message, but in this context it's enough just to test that
//       --subprocess_max_message_size_bytes=0 isn't treated literally as 0
//       for the maximum size of SubprocessResponsePB messages
TEST_F(SubprocessServerTest, UnlimitedPayloadSize) {
  // Set a short timeout to speed up testing.
  FLAGS_subprocess_timeout_secs = 1;

  // No upper limit on protobuf-encoded responses.
  FLAGS_subprocess_max_message_size_bytes = 0;
  ASSERT_OK(ResetSubprocessServer());

  // An empty message.
  {
    auto req = CreateEchoSubprocessRequestPB("");
    SubprocessResponsePB res;
    ASSERT_OK(server_->Execute(&req, &res));
  }

  // A very short message.
  {
    auto req = CreateEchoSubprocessRequestPB("x");
    SubprocessResponsePB res;
    ASSERT_OK(server_->Execute(&req, &res));
  }

  // Just in case, send a longer message: should be OK as well.
  {
    auto req = CreateEchoSubprocessRequestPB(string(1024, 'x'));
    SubprocessResponsePB res;
    ASSERT_OK(server_->Execute(&req, &res));
  }
}

// Check cases where the message is large enough to not fit in the pipe to be
// transferred in one single pipe message transmission.
TEST_F(SubprocessServerTest, LargePayloadSize) {
  // Set a short timeout to speed up testing.
  FLAGS_subprocess_timeout_secs = 5;
  // Set the max message to 24MB (3x the default size)
  FLAGS_subprocess_max_message_size_bytes =  24 * 1024 * 1024;
  ASSERT_OK(ResetSubprocessServer(0, 0, 24 * 1024 * 1024));

  // Send in a large message that isn't oversized as per the current limit.
  {
    auto req = CreateEchoSubprocessRequestPB(string(23 * 1024 * 1024, 'x'));
    SubprocessResponsePB res;
    ASSERT_OK(server_->Execute(&req, &res));
  }

  // Send a large oversized message.
  {
    auto req = CreateEchoSubprocessRequestPB(string(24 * 1024 * 1024, 'x'));
    SubprocessResponsePB res;
    const auto s = server_->Execute(&req, &res);

    // The request will timeout because the oversized response is read and
    // discarded, and there isn't any application-level data to be sent back.
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "timed out while in flight");
  }

  // Non-oversized follow-up messages should be received without any issues:
  // the communication channel should be cleared of any oversized requests
  // sent earlier.
  {
    auto req = CreateEchoSubprocessRequestPB(string(23 * 1024 * 1024, 'x'));
    SubprocessResponsePB res;
    ASSERT_OK(server_->Execute(&req, &res));
  }
}

} // namespace subprocess
} // namespace kudu

