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

#include "kudu/util/threadpool.h"

#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/metrics.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"
#include "kudu/util/trace_metrics.h"

namespace kudu {

using std::deque;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

////////////////////////////////////////////////////////
// ThreadPoolBuilder
////////////////////////////////////////////////////////

ThreadPoolBuilder::ThreadPoolBuilder(string name)
    : name_(std::move(name)),
      min_threads_(0),
      max_threads_(base::NumCPUs()),
      max_queue_size_(std::numeric_limits<int>::max()),
      idle_timeout_(MonoDelta::FromMilliseconds(500)),
      enable_scheduler_(false),
      schedule_period_ms_(100) {}

ThreadPoolBuilder& ThreadPoolBuilder::set_trace_metric_prefix(const string& prefix) {
  trace_metric_prefix_ = prefix;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_min_threads(int min_threads) {
  CHECK_GE(min_threads, 0);
  min_threads_ = min_threads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_threads(int max_threads) {
  CHECK_GT(max_threads, 0);
  max_threads_ = max_threads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_queue_size(int max_queue_size) {
  max_queue_size_ = max_queue_size;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_idle_timeout(const MonoDelta& idle_timeout) {
  idle_timeout_ = idle_timeout;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_metrics(ThreadPoolMetrics metrics) {
  metrics_ = std::move(metrics);
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_enable_scheduler() {
  enable_scheduler_ = true;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_schedule_period_ms(uint32_t schedule_period_ms) {
  schedule_period_ms_ = schedule_period_ms;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_queue_overload_threshold(
    const MonoDelta& threshold) {
  queue_overload_threshold_ = threshold;
  return *this;
}

Status ThreadPoolBuilder::Build(unique_ptr<ThreadPool>* pool) const {
  pool->reset(new ThreadPool(*this));
  return (*pool)->Init();
}

SchedulerThread::SchedulerThread(string thread_pool_name, uint32_t schedule_period_ms)
    : thread_pool_name_(std::move(thread_pool_name)),
      schedule_period_ms_(schedule_period_ms),
      shutdown_(1) {}

SchedulerThread::~SchedulerThread() {
  if (thread_) {
    Shutdown();
  }
}

Status SchedulerThread::Start() {
  return Thread::Create(
      thread_pool_name_, "scheduler", [this]() { this->RunLoop(); }, &thread_);
}

Status SchedulerThread::Shutdown() {
  if (thread_) {
    shutdown_.CountDown();
    thread_->Join();
  }
  return Status::OK();
}

void SchedulerThread::RunLoop() {
  while (!shutdown_.WaitFor(MonoDelta::FromMilliseconds(schedule_period_ms_))) {
    MonoTime now = MonoTime::Now();
    vector<SchedulerTask> pending_tasks;
    {
      MutexLock auto_lock(mutex_);
      auto upper_it = future_tasks_.upper_bound(now);
      for (auto it = future_tasks_.begin(); it != upper_it; it++) {
        pending_tasks.emplace_back(std::move(it->second));
      }
      future_tasks_.erase(future_tasks_.begin(), upper_it);
    }

    for (const auto& task : pending_tasks) {
      ThreadPoolToken* token = task.thread_pool_token;
      while (token != nullptr) {
        Status s = token->Submit(task.f);
        if (s.ok()) {
          break;
        }
        DCHECK(s.IsServiceUnavailable())
            << Substitute("threadpool token Submit status: $0", s.ToString());

        if (!token->MaySubmitNewTasks()) {
          // threadpool token is Shutdown, skip the task.
          break;
        }
        // If developers use ThreadPoolToken::Schedule(...) too frequent, blocking queue's
        // capacity will be full, then retry submit the task again.
        VLOG(1) << Substitute("threadpool token Submit status: $0, retry the task", s.ToString());
        SleepFor(MonoDelta::FromMilliseconds(1));
      }
    }
  }
}

////////////////////////////////////////////////////////
// ThreadPoolToken
////////////////////////////////////////////////////////

ThreadPoolToken::ThreadPoolToken(ThreadPool* pool,
                                 ThreadPool::ExecutionMode mode,
                                 ThreadPoolMetrics metrics)
    : mode_(mode),
      metrics_(std::move(metrics)),
      pool_(pool),
      state_(State::IDLE),
      not_running_cond_(&pool->lock_),
      active_threads_(0) {
}

ThreadPoolToken::~ThreadPoolToken() {
  Shutdown();
  pool_->ReleaseToken(this);
}

Status ThreadPoolToken::Submit(std::function<void()> f) {
  return pool_->DoSubmit(std::move(f), this);
}

void ThreadPoolToken::Shutdown() {
  MutexLock unique_lock(pool_->lock_);
  pool_->CheckNotPoolThreadUnlocked();

  // Clear the queue under the lock, but defer the releasing of the tasks
  // outside the lock, in case there are concurrent threads wanting to access
  // the ThreadPool. The task's destructors may acquire locks, etc, so this
  // also prevents lock inversions.
  std::deque<ThreadPool::Task> to_release = std::move(entries_);
  pool_->total_queued_tasks_ -= to_release.size();

  switch (state()) {
    case State::IDLE:
      // There were no tasks outstanding; we can quiesce the token immediately.
      Transition(State::QUIESCED);
      break;
    case State::RUNNING:
      // There were outstanding tasks. If any are still running, switch to
      // QUIESCING and wait for them to finish (the worker thread executing
      // the token's last task will switch the token to QUIESCED). Otherwise,
      // we can quiesce the token immediately.

      // Note: this is an O(n) operation, but it's expected to be infrequent.
      // Plus doing it this way (rather than switching to QUIESCING and waiting
      // for a worker thread to process the queue entry) helps retain state
      // transition symmetry with ThreadPool::Shutdown.
      for (auto it = pool_->queue_.begin(); it != pool_->queue_.end();) {
        if (*it == this) {
          it = pool_->queue_.erase(it);
        } else {
          it++;
        }
      }

      if (active_threads_ == 0) {
        Transition(State::QUIESCED);
        break;
      }
      Transition(State::QUIESCING);
      [[fallthrough]];
    case State::QUIESCING:
      // The token is already quiescing. Just wait for a worker thread to
      // switch it to QUIESCED.
      while (state() != State::QUIESCED) {
        not_running_cond_.Wait();
      }
      break;
    default:
      break;
  }

  // Finally release the queued tasks, outside the lock.
  unique_lock.Unlock();
  for (auto& t : to_release) {
    if (t.trace) {
      t.trace->Release();
    }
  }
}

// Submit a task, running after delay_ms delay some time
Status ThreadPoolToken::Schedule(std::function<void()> f, int64_t delay_ms) {
  if (PREDICT_FALSE(!MaySubmitNewTasks())) {
    return Status::ServiceUnavailable("Thread pool token was shut down");
  }
  CHECK(mode() == ThreadPool::ExecutionMode::SERIAL);
  MonoTime excute_time = MonoTime::Now();
  excute_time.AddDelta(MonoDelta::FromMilliseconds(delay_ms));
  pool_->scheduler()->Schedule(this, std::move(f), excute_time);
  return Status::OK();
}

void ThreadPoolToken::Wait() {
  MutexLock unique_lock(pool_->lock_);
  pool_->CheckNotPoolThreadUnlocked();
  while (IsActive()) {
    not_running_cond_.Wait();
  }
}

bool ThreadPoolToken::WaitUntil(const MonoTime& until) {
  MutexLock unique_lock(pool_->lock_);
  pool_->CheckNotPoolThreadUnlocked();
  while (IsActive()) {
    if (!not_running_cond_.WaitUntil(until)) {
      return false;
    }
  }
  return true;
}

bool ThreadPoolToken::WaitFor(const MonoDelta& delta) {
  return WaitUntil(MonoTime::Now() + delta);
}

void ThreadPoolToken::Transition(State new_state) {
#ifndef NDEBUG
  CHECK_NE(state_, new_state);

  switch (state_) {
    case State::IDLE:
      CHECK(new_state == State::RUNNING ||
            new_state == State::QUIESCED);
      if (new_state == State::RUNNING) {
        CHECK(!entries_.empty());
      } else {
        CHECK(entries_.empty());
        CHECK_EQ(active_threads_, 0);
      }
      break;
    case State::RUNNING:
      CHECK(new_state == State::IDLE ||
            new_state == State::QUIESCING ||
            new_state == State::QUIESCED);
      CHECK(entries_.empty());
      if (new_state == State::QUIESCING) {
        CHECK_GT(active_threads_, 0);
      }
      break;
    case State::QUIESCING:
      CHECK(new_state == State::QUIESCED);
      CHECK_EQ(active_threads_, 0);
      break;
    case State::QUIESCED:
      CHECK(false); // QUIESCED is a terminal state
      break;
    default:
      LOG(FATAL) << "Unknown token state: " << state_;
  }
#endif

  // Take actions based on the state we're entering.
  switch (new_state) {
    case State::IDLE:
    case State::QUIESCED:
      not_running_cond_.Broadcast();
      break;
    default:
      break;
  }

  state_ = new_state;
}

const char* ThreadPoolToken::StateToString(State s) {
  switch (s) {
    case State::IDLE: return "IDLE"; break;
    case State::RUNNING: return "RUNNING"; break;
    case State::QUIESCING: return "QUIESCING"; break;
    case State::QUIESCED: return "QUIESCED"; break;
  }
  return "<cannot reach here>";
}

////////////////////////////////////////////////////////
// ThreadPool
////////////////////////////////////////////////////////

ThreadPool::ThreadPool(const ThreadPoolBuilder& builder)
    : name_(builder.name_),
      min_threads_(builder.min_threads_),
      max_threads_(builder.max_threads_),
      max_queue_size_(builder.max_queue_size_),
      idle_timeout_(builder.idle_timeout_),
      pool_status_(Status::Uninitialized("The pool was not initialized.")),
      idle_cond_(&lock_),
      no_threads_cond_(&lock_),
      num_threads_(0),
      num_threads_pending_start_(0),
      active_threads_(0),
      total_queued_tasks_(0),
      tokenless_(NewToken(ExecutionMode::CONCURRENT)),
      metrics_(builder.metrics_),
      scheduler_(nullptr),
      schedule_period_ms_(builder.schedule_period_ms_),
      enable_scheduler_(builder.enable_scheduler_) {
  string prefix = !builder.trace_metric_prefix_.empty() ?
      builder.trace_metric_prefix_ : builder.name_;

  queue_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".queue_time_us");
  run_wall_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".run_wall_time_us");
  run_cpu_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".run_cpu_time_us");

  const auto& ovt = builder.queue_overload_threshold_;
  if (ovt.Initialized() && ovt.ToNanoseconds() > 0) {
    load_meter_.reset(new QueueLoadMeter(*this, ovt, max_threads_));
  }
}

ThreadPool::~ThreadPool() {
  // There should only be one live token: the one used in tokenless submission.
  CHECK_EQ(1, tokens_.size()) << Substitute(
      "Threadpool $0 destroyed with $1 allocated tokens",
      name_, tokens_.size());
  Shutdown();
}

Status ThreadPool::Init() {
  if (!pool_status_.IsUninitialized()) {
    return Status::NotSupported("The thread pool is already initialized");
  }
  pool_status_ = Status::OK();
  num_threads_pending_start_ = min_threads_;
  for (int i = 0; i < min_threads_; i++) {
    Status status = CreateThread();
    if (!status.ok()) {
      Shutdown();
      return status;
    }
  }
  if (enable_scheduler_) {
    scheduler_ = new SchedulerThread(name_, schedule_period_ms_);
    RETURN_NOT_OK(scheduler_->Start());
  }
  return Status::OK();
}

void ThreadPool::Shutdown() {
  if (scheduler_) {
    scheduler_->Shutdown();
    delete scheduler_;
    scheduler_ = nullptr;
  }

  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  // Note: this is the same error seen at submission if the pool is at
  // capacity, so clients can't tell them apart. This isn't really a practical
  // concern though because shutting down a pool typically requires clients to
  // be quiesced first, so there's no danger of a client getting confused.
  pool_status_ = Status::ServiceUnavailable("The pool has been shut down.");

  // Clear the various queues under the lock, but defer the releasing
  // of the tasks outside the lock, in case there are concurrent threads
  // wanting to access the ThreadPool. The task's destructors may acquire
  // locks, etc, so this also prevents lock inversions.
  queue_.clear();
  std::deque<std::deque<Task>> to_release;
  for (auto* t : tokens_) {
    if (!t->entries_.empty()) {
      to_release.emplace_back(std::move(t->entries_));
    }
    switch (t->state()) {
      case ThreadPoolToken::State::IDLE:
        // The token is idle; we can quiesce it immediately.
        t->Transition(ThreadPoolToken::State::QUIESCED);
        break;
      case ThreadPoolToken::State::RUNNING:
        // The token has tasks associated with it. If they're merely queued
        // (i.e. there are no active threads), the tasks will have been removed
        // above and we can quiesce immediately. Otherwise, we need to wait for
        // the threads to finish.
        t->Transition(t->active_threads_ > 0 ?
            ThreadPoolToken::State::QUIESCING :
            ThreadPoolToken::State::QUIESCED);
        break;
      default:
        break;
    }
  }

  // The queues are empty. Wake any sleeping worker threads and wait for all
  // of them to exit. Some worker threads will exit immediately upon waking,
  // while others will exit after they finish executing an outstanding task.
  total_queued_tasks_ = 0;
  while (!idle_threads_.empty()) {
    idle_threads_.front().not_empty.Signal();
    idle_threads_.pop_front();
  }
  while (num_threads_ + num_threads_pending_start_ > 0) {
    no_threads_cond_.Wait();
  }

  // All the threads have exited. Check the state of each token.
  for (auto* t : tokens_) {
    DCHECK(t->state() == ThreadPoolToken::State::IDLE ||
           t->state() == ThreadPoolToken::State::QUIESCED);
  }

  // Finally release the queued tasks, outside the lock.
  unique_lock.Unlock();
  for (auto& token : to_release) {
    for (auto& t : token) {
      if (t.trace) {
        t.trace->Release();
      }
    }
  }
}

unique_ptr<ThreadPoolToken> ThreadPool::NewToken(ExecutionMode mode) {
  return NewTokenWithMetrics(mode, {});
}

unique_ptr<ThreadPoolToken> ThreadPool::NewTokenWithMetrics(
    ExecutionMode mode, ThreadPoolMetrics metrics) {
  MutexLock guard(lock_);
  unique_ptr<ThreadPoolToken> t(new ThreadPoolToken(this,
                                                    mode,
                                                    std::move(metrics)));
  InsertOrDie(&tokens_, t.get());
  return t;
}

bool ThreadPool::QueueOverloaded(MonoDelta* overloaded_time,
                                 MonoDelta* threshold) const {
  if (!load_meter_) {
    return false;
  }
  if (threshold) {
    *threshold = load_meter_->queue_time_threshold();
  }
  return load_meter_->overloaded(overloaded_time);
}

void ThreadPool::ReleaseToken(ThreadPoolToken* t) {
  MutexLock guard(lock_);
  CHECK(!t->IsActive()) << Substitute("Token with state $0 may not be released",
                                      ThreadPoolToken::StateToString(t->state()));
  CHECK_EQ(1, tokens_.erase(t));
}

Status ThreadPool::Submit(std::function<void()> f) {
  return DoSubmit(std::move(f), tokenless_.get());
}

Status ThreadPool::DoSubmit(std::function<void()> f, ThreadPoolToken* token) {
  DCHECK(token);
  const MonoTime submit_time = MonoTime::Now();

  MutexLock guard(lock_);
  if (PREDICT_FALSE(!pool_status_.ok())) {
    return pool_status_;
  }

  if (PREDICT_FALSE(!token->MaySubmitNewTasks())) {
    return Status::ServiceUnavailable("Thread pool token was shut down");
  }

  // Size limit check.
  int64_t capacity_remaining = static_cast<int64_t>(max_threads_) - active_threads_ +
                               static_cast<int64_t>(max_queue_size_) - total_queued_tasks_;
  if (capacity_remaining < 1) {
    return Status::ServiceUnavailable(
        Substitute("Thread pool is at capacity ($0/$1 tasks running, $2/$3 tasks queued)",
                   num_threads_ + num_threads_pending_start_, max_threads_,
                   total_queued_tasks_, max_queue_size_));
  }

  // Should we create another thread?

  // We assume that each current inactive thread will grab one item from the
  // queue.  If it seems like we'll need another thread, we create one.
  //
  // Rather than creating the thread here, while holding the lock, we defer
  // it to down below. This is because thread creation can be rather slow
  // (hundreds of milliseconds in some cases) and we'd like to allow the
  // existing threads to continue to process tasks while we do so.
  //
  // In theory, a currently active thread could finish immediately after this
  // calculation but before our new worker starts running. This would mean we
  // created a thread we didn't really need. However, this race is unavoidable
  // and harmless.
  //
  // Of course, we never create more than max_threads_ threads no matter what.
  int threads_from_this_submit =
      token->IsActive() && token->mode() == ExecutionMode::SERIAL ? 0 : 1;
  int inactive_threads = num_threads_ + num_threads_pending_start_ - active_threads_;
  int additional_threads = static_cast<int>(queue_.size())
                         + threads_from_this_submit
                         - inactive_threads;
  bool need_a_thread = false;
  if (additional_threads > 0 && num_threads_ + num_threads_pending_start_ < max_threads_) {
    need_a_thread = true;
    num_threads_pending_start_++;
  }

  Task task;
  task.func = std::move(f);
  task.trace = Trace::CurrentTrace();
  // Need to AddRef, since the thread which submitted the task may go away,
  // and we don't want the trace to be destructed while waiting in the queue.
  if (task.trace) {
    task.trace->AddRef();
  }
  task.submit_time = submit_time;

  // Add the task to the token's queue.
  ThreadPoolToken::State state = token->state();
  DCHECK(state == ThreadPoolToken::State::IDLE ||
         state == ThreadPoolToken::State::RUNNING);
  token->entries_.emplace_back(std::move(task));
  if (state == ThreadPoolToken::State::IDLE ||
      token->mode() == ExecutionMode::CONCURRENT) {
    queue_.emplace_back(token);
    if (state == ThreadPoolToken::State::IDLE) {
      token->Transition(ThreadPoolToken::State::RUNNING);
    }
  }
  int length_at_submit = total_queued_tasks_++;

  // Wake up an idle thread for this task. Choosing the thread at the front of
  // the list ensures LIFO semantics as idling threads are also added to the front.
  //
  // If there are no idle threads, the new task remains on the queue and is
  // processed by an active thread (or a thread we're about to create) at some
  // point in the future.
  if (!idle_threads_.empty()) {
    idle_threads_.front().not_empty.Signal();
    idle_threads_.pop_front();
  }
  NotifyLoadMeterUnlocked();

  // Update the token's queue length histogram under a lock. Submitting a task
  // via a pool token and destroying the token might be run concurrently by
  // differen threads. If updating the metric after releasing the 'lock_', the
  // destructor for the token might run after 'lock_' is released since the
  // other thread might already be executing the code of the ThreadPoolToken's
  // destructor, just waiting to acquire 'lock_' in ThreadPoolToken::Shutdown().
  if (token->metrics_.queue_length_histogram) {
    token->metrics_.queue_length_histogram->Increment(length_at_submit);
  }

  guard.Unlock();

  if (metrics_.queue_length_histogram) {
    metrics_.queue_length_histogram->Increment(length_at_submit);
  }

  if (need_a_thread) {
    Status status = CreateThread();
    if (!status.ok()) {
      guard.Lock();
      num_threads_pending_start_--;
      if (num_threads_ + num_threads_pending_start_ == 0) {
        // If we have no threads, we can't do any work.
        return status;
      }
      // If we failed to create a thread, but there are still some other
      // worker threads, log a warning message and continue.
      LOG(ERROR) << "Thread pool failed to create thread: "
                 << status.ToString();
    }
  }

  return Status::OK();
}

void ThreadPool::Wait() {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  while (total_queued_tasks_ > 0 || active_threads_ > 0) {
    idle_cond_.Wait();
  }
}

void ThreadPool::WaitForScheduler() {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  // Generally, ignore scheduler's pending tasks, but
  // set ScheduledTaskWaitType::WAIT at unit tests.
  bool wait_scheduler = (scheduler_ != nullptr);
  while (total_queued_tasks_ > 0 || active_threads_ > 0 ||
        (wait_scheduler && !scheduler_->empty())) {
    idle_cond_.Wait();
  }
}

bool ThreadPool::WaitUntil(const MonoTime& until) {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  while (total_queued_tasks_ > 0 || active_threads_ > 0) {
    if (!idle_cond_.WaitUntil(until)) {
      return false;
    }
  }
  return true;
}

bool ThreadPool::WaitFor(const MonoDelta& delta) {
  return WaitUntil(MonoTime::Now() + delta);
}

void ThreadPool::DispatchThread() {
  MutexLock unique_lock(lock_);
  InsertOrDie(&threads_, Thread::current_thread());
  DCHECK_GT(num_threads_pending_start_, 0);
  num_threads_++;
  num_threads_pending_start_--;
  // If we are one of the first 'min_threads_' to start, we must be
  // a "permanent" thread.
  bool permanent = num_threads_ <= min_threads_;

  // Owned by this worker thread and added/removed from idle_threads_ as needed.
  IdleThread me(&lock_);

  while (true) {
    // Note: Status::Aborted() is used to indicate normal shutdown.
    if (!pool_status_.ok()) {
      VLOG(2) << "DispatchThread exiting: " << pool_status_.ToString();
      break;
    }

    if (queue_.empty()) {
      // There's no work to do, let's go idle.
      //
      // Note: if FIFO behavior is desired, it's as simple as changing this to push_back().
      idle_threads_.push_front(me);
      NotifyLoadMeterUnlocked();
      SCOPED_CLEANUP({
        // For some wake ups (i.e. Shutdown or DoSubmit) this thread is
        // guaranteed to be unlinked after being awakened. In others (i.e.
        // spurious wake-up or Wait timeout), it'll still be linked.
        if (me.is_linked()) {
          idle_threads_.erase(idle_threads_.iterator_to(me));
        }
      });
      if (permanent) {
        me.not_empty.Wait();
      } else {
        if (!me.not_empty.WaitFor(idle_timeout_)) {
          // After much investigation, it appears that pthread condition variables have
          // a weird behavior in which they can return ETIMEDOUT from timed_wait even if
          // another thread did in fact signal. Apparently after a timeout there is some
          // brief period during which another thread may actually grab the internal mutex
          // protecting the state, signal, and release again before we get the mutex. So,
          // we'll recheck the empty queue case regardless.
          if (queue_.empty()) {
            VLOG(3) << "Releasing worker thread from pool " << name_ << " after "
                    << idle_timeout_.ToMilliseconds() << "ms of idle time.";
            break;
          }
        }
      }
      continue;
    }

    // Get the next token and task to execute.
    ThreadPoolToken* token = queue_.front();
    queue_.pop_front();
    DCHECK_EQ(ThreadPoolToken::State::RUNNING, token->state());
    DCHECK(!token->entries_.empty());
    Task task = std::move(token->entries_.front());
    token->entries_.pop_front();
    token->active_threads_++;
    --total_queued_tasks_;
    ++active_threads_;

    const MonoTime now(MonoTime::Now());
    const MonoDelta queue_time = now - task.submit_time;
    NotifyLoadMeterUnlocked(queue_time);

    unique_lock.Unlock();

    // Release the reference which was held by the queued item.
    ADOPT_TRACE(task.trace);
    if (task.trace) {
      task.trace->Release();
    }

    // Update metrics.
    const int64_t queue_time_us = queue_time.ToMicroseconds();
    TRACE_COUNTER_INCREMENT(queue_time_trace_metric_name_, queue_time_us);
    if (metrics_.queue_time_us_histogram) {
      metrics_.queue_time_us_histogram->Increment(queue_time_us);
    }
    if (token->metrics_.queue_time_us_histogram) {
      token->metrics_.queue_time_us_histogram->Increment(queue_time_us);
    }

    // Execute the task
    {
      MicrosecondsInt64 start_wall_us = GetMonoTimeMicros();
      MicrosecondsInt64 start_cpu_us = GetThreadCpuTimeMicros();

      task.func();

      int64_t wall_us = GetMonoTimeMicros() - start_wall_us;
      int64_t cpu_us = GetThreadCpuTimeMicros() - start_cpu_us;

      if (metrics_.run_time_us_histogram) {
        metrics_.run_time_us_histogram->Increment(wall_us);
      }
      if (token->metrics_.run_time_us_histogram) {
        token->metrics_.run_time_us_histogram->Increment(wall_us);
      }
      TRACE_COUNTER_INCREMENT(run_wall_time_trace_metric_name_, wall_us);
      TRACE_COUNTER_INCREMENT(run_cpu_time_trace_metric_name_, cpu_us);
    }
    // Destruct the task while we do not hold the lock.
    //
    // The task's destructor may be expensive if it has a lot of bound
    // objects, and we don't want to block submission of the threadpool.
    // In the worst case, the destructor might even try to do something
    // with this threadpool, and produce a deadlock.
    task.func = nullptr;
    unique_lock.Lock();

    // Possible states:
    // 1. The token was shut down while we ran its task. Transition to QUIESCED.
    // 2. The token has no more queued tasks. Transition back to IDLE.
    // 3. The token has more tasks. Requeue it and transition back to RUNNABLE.
    ThreadPoolToken::State state = token->state();
    DCHECK(state == ThreadPoolToken::State::RUNNING ||
           state == ThreadPoolToken::State::QUIESCING);
    if (--token->active_threads_ == 0) {
      if (state == ThreadPoolToken::State::QUIESCING) {
        DCHECK(token->entries_.empty());
        token->Transition(ThreadPoolToken::State::QUIESCED);
      } else if (token->entries_.empty()) {
        token->Transition(ThreadPoolToken::State::IDLE);
      } else if (token->mode() == ExecutionMode::SERIAL) {
        queue_.emplace_back(token);
      }
    }
    if (!queue_.empty()) {
      // If the queue is empty, the LoadMeter is notified on next iteration of
      // the outer while() loop under the 'if (queue_.empty())' clause. Here
      // it's crucial to call NotifyLoadMeter() _before_ decrementing
      // 'active_threads_' to avoid reporting this thread as a spare one despite
      // the fact that it will run a task from the non-empty queue immediately
      // on next iteration of the outer while() loop.
      NotifyLoadMeterUnlocked();
    }
    if (--active_threads_ == 0) {
      idle_cond_.Broadcast();
    }
  }

  // It's important that we hold the lock between exiting the loop and dropping
  // num_threads_. Otherwise it's possible someone else could come along here
  // and add a new task just as the last running thread is about to exit.
  CHECK(unique_lock.OwnsLock());

  CHECK_EQ(threads_.erase(Thread::current_thread()), 1);
  num_threads_--;
  if (num_threads_ + num_threads_pending_start_ == 0) {
    no_threads_cond_.Broadcast();

    // Sanity check: if we're the last thread exiting, the queue ought to be
    // empty. Otherwise it will never get processed.
    CHECK(queue_.empty());
    DCHECK_EQ(0, total_queued_tasks_);
  }
}

Status ThreadPool::CreateThread() {
  return kudu::Thread::Create("thread pool", strings::Substitute("$0 [worker]", name_),
                              [this]() { this->DispatchThread(); }, nullptr);
}

void ThreadPool::CheckNotPoolThreadUnlocked() {
  Thread* current = Thread::current_thread();
  if (ContainsKey(threads_, current)) {
    LOG(FATAL) << Substitute("Thread belonging to thread pool '$0' with "
        "name '$1' called pool function that would result in deadlock",
        name_, current->name());
  }
}

void ThreadPool::NotifyLoadMeterUnlocked(const MonoDelta& queue_time) {
  if (!load_meter_) {
    return;
  }

  lock_.AssertAcquired();
  MonoTime queue_head_submit_time;
  if (!queue_.empty()) {
    DCHECK(!queue_.front()->entries_.empty());
    queue_head_submit_time = queue_.front()->entries_.front().submit_time;
  }
  load_meter_->UpdateQueueInfoUnlocked(queue_time,
                                       queue_head_submit_time,
                                       active_threads_ < max_threads_);
}

////////////////////////////////////////////////////////
// ThreadPool::QueueLoadMeter
////////////////////////////////////////////////////////

ThreadPool::QueueLoadMeter::QueueLoadMeter(
    const ThreadPool& tpool,
    const MonoDelta& queue_time_threshold,
    size_t queue_time_history_length)
    : tpool_(tpool),
      queue_time_threshold_(queue_time_threshold),
      queue_time_history_length_(queue_time_history_length),
      over_queue_threshold_num_(0),
      queue_head_submit_time_(MonoTime()),
      overloaded_since_(MonoTime()),
      has_spare_thread_(true) {
}

bool ThreadPool::QueueLoadMeter::overloaded(MonoDelta* time_overloaded) {
  // First, check whether the queue was overloaded upon the latest update
  // on the queue from the ThreadPool's activity. If that's the case, there is
  // no need to dig further.
  MonoTime overloaded_since = overloaded_since_.load();
  if (overloaded_since.Initialized()) {
    if (time_overloaded) {
      *time_overloaded = MonoTime::Now() - overloaded_since;
    }
    return true;
  }

  // Even if the queue was not overloaded upon the latest update on the
  // ThreadPool's activity, the queue might have stalled because all its worker
  // threads have been busy for long time. If so, 'overloaded_since_' hasn't
  // been updated by the activity on the thread pool itself. However, it's
  // possible to detect whether the queue has stalled by checking if the task
  // at the head of the queue hasn't been dispatched for longer than
  // queue_time_threshold_ time interval.
  MonoTime queue_head_submit_time = queue_head_submit_time_.load();
  if (!queue_head_submit_time.Initialized() || has_spare_thread_.load()) {
    return false;
  }

  const auto now = MonoTime::Now();
  const MonoDelta queue_time = now - queue_head_submit_time;
  if (queue_time > queue_time_threshold_) {
    MonoTime overloaded_since;
    if (overloaded_since_.compare_exchange_strong(overloaded_since, now)) {
      VLOG(3) << Substitute("state transition: normal --> overloaded");
      if (time_overloaded) {
        *time_overloaded = queue_time - queue_time_threshold_;
      }
      return true;
    }
    DCHECK(overloaded_since.Initialized());
    if (time_overloaded) {
      *time_overloaded = now - overloaded_since;
    }
    return true;
  }

  return false;
}

void ThreadPool::QueueLoadMeter::UpdateQueueInfoUnlocked(
    const MonoDelta& task_queue_time,
    const MonoTime& queue_head_submit_time,
    bool has_spare_thread) {
  tpool_.lock_.AssertAcquired();

  if (task_queue_time.Initialized()) {
    queue_times_.emplace_back(task_queue_time);
    // Given the criterion to detect whether the queue is overloaded, it's not
    // exactly necessary to track the running minimum of queue times for the
    // specified history window. It's enough just to keep an eye on whether the
    // window contains at least one element that's not over the threshold.
    if (task_queue_time > queue_time_threshold_) {
      ++over_queue_threshold_num_;
    }
    if (queue_times_.size() > queue_time_history_length_) {
      const auto& elem = queue_times_.front();
      if (elem > queue_time_threshold_) {
        --over_queue_threshold_num_;
      }
      queue_times_.pop_front();
    }
  }
  queue_head_submit_time_.store(queue_head_submit_time);
  has_spare_thread_.store(has_spare_thread);

  // Update the load state of the queue, storing appropriate information
  // in the 'overloaded_since_' field.
  const auto now = MonoTime::Now();
  const bool queue_empty = !queue_head_submit_time.Initialized();
  const auto queue_time = queue_empty
      ? MonoDelta::FromSeconds(0) : now - queue_head_submit_time;
  const bool was_overloaded = overloaded_since_.load().Initialized();
  const bool overloaded = !has_spare_thread &&
      (over_queue_threshold_num_ == queue_time_history_length_ ||
       queue_time > queue_time_threshold_);
  // Track the state transitions and update overloaded_since_.
  if (!was_overloaded && overloaded) {
    VLOG(3) << Substitute("state transition: normal --> overloaded");
    overloaded_since_.store(now);
  } else if (was_overloaded && !overloaded) {
    VLOG(3) << Substitute("state transition: overloaded --> normal");
    overloaded_since_.store(MonoTime());
  }
  VLOG(4) << Substitute("state refreshed: overloaded since $0, queue $1, "
                        "has $2 thread, queue head submit time $3, "
                        "queue time $4",
                        overloaded_since_.load().ToString(),
                        queue_empty ? "empty" : "not empty",
                        has_spare_thread ? "spare" : "no spare",
                        queue_head_submit_time.ToString(),
                        queue_time.ToString());
}

std::ostream& operator<<(std::ostream& o, ThreadPoolToken::State s) {
  return o << ThreadPoolToken::StateToString(s);
}

} // namespace kudu
