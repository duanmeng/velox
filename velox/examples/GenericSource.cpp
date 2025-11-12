/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "GenericSource.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/init/Init.h>
#include <vector/fuzzer/VectorFuzzer.h>

#include "velox/exec/BlockingReason.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace boltrain {
/// A simple demo of a 1P-1C asynchronous scenario.
class SimpleStringTest {
 public:
  explicit SimpleStringTest(folly::Executor* executor) : executor_(executor) {}

  void run() {
    LOG(ERROR) << "\n--- Running Simple 1P-1C Asynchronous Demo ---"
               << std::endl;
    const auto source = createLocalGenericSource<std::shared_ptr<std::string>>(
        "SimpleStringTestSource", 2);
    folly::Promise<folly::Unit> demoCompletePromise;
    auto demoCompleteFuture = demoCompletePromise.getFuture();
    produceAsync(source, 0);
    consumeAsync(source, demoCompletePromise);
    LOG(ERROR) << "Waiting for demo to complete..." << std::endl;
    demoCompleteFuture.wait();
    LOG(ERROR) << "Simple Demo finished." << std::endl;
  }

 private:
  void produceAsync(
      std::shared_ptr<GenericSource<std::shared_ptr<std::string>>> source,
      int index) {
    const int kTotalMessages = 5;
    ContinueFuture future;
    if (index >= kTotalMessages) {
      LOG(ERROR) << "[Producer] All messages sent. Enqueuing end signal."
                 << std::endl;
      source->enqueue(nullptr, &future);
      return;
    }
    auto message = std::make_shared<std::string>(
        "Hello from producer, message #" + std::to_string(index));
    LOG(ERROR) << "[Producer] Trying to enqueue: '" << *message << "'"
               << std::endl;
    const auto reason = source->enqueue(message, &future);
    if (reason == BlockingReason::kNotBlocked) {
      produceAsync(source, index + 1);
    } else {
      LOG(ERROR) << "[Producer] Queue is full. Will resume when notified."
                 << std::endl;
      std::move(future).via(executor_).thenValue(
          [this, source, index](folly::Unit) {
            LOG(ERROR) << "[Producer] Resuming work..." << std::endl;
            produceAsync(source, index); // Retry the same index
          });
    }
  }

  void consumeAsync(
      std::shared_ptr<GenericSource<std::shared_ptr<std::string>>> source,
      folly::Promise<folly::Unit>& demoCompletePromise) {
    ContinueFuture future;
    std::shared_ptr<std::string> receivedMessage;
    const auto reason = source->next(receivedMessage, &future);
    if (reason == BlockingReason::kNotBlocked) {
      if (!receivedMessage) {
        LOG(ERROR) << "[Consumer] Received end signal. Demo is complete."
                   << std::endl;
        demoCompletePromise.setValue();
        return;
      }
      LOG(ERROR) << "[Consumer] Successfully received: '" << *receivedMessage
                 << "'" << std::endl;
      consumeAsync(source, demoCompletePromise);
    } else {
      LOG(ERROR) << "[Consumer] Queue is empty. Will resume when notified."
                 << std::endl;
      std::move(future).via(executor_).thenValue(
          [this, source, &demoCompletePromise](folly::Unit) {
            LOG(ERROR) << "[Consumer] Resuming work..." << std::endl;
            consumeAsync(source, demoCompletePromise);
          });
    }
  }
  folly::Executor* const executor_;
};

/// Encapsulates the asynchronous test for GenericSource with std::string.
class StringTest {
 public:
  explicit StringTest(
      folly::Executor* executor,
      int numProducers = 1,
      int numConsumers = 1)
      : executor_(executor),
        numProducers_(numProducers),
        numConsumers_(numConsumers),
        producersFinished_(0),
        consumersFinished_(numConsumers) {}

  void run() {
    LOG(ERROR) << "\n--- Running Async String Test (" << numProducers_ << "P, "
               << numConsumers_ << "C) ---";
    const auto source = createLocalGenericSource<std::shared_ptr<std::string>>(
        "StringTestSource", 3);
    folly::Promise<folly::Unit> testComplete;
    auto testCompleteFuture = testComplete.getFuture();
    for (int i = 0; i < numConsumers_; ++i) {
      consumeAsync(source, testComplete, i);
    }
    for (int i = 0; i < numProducers_; ++i) {
      produceAsync(source, i);
    }
    testCompleteFuture.wait();
    LOG(ERROR) << "--- Async String Test Finished ---\n";
  }

 private:
  void produceAsync(
      std::shared_ptr<GenericSource<std::shared_ptr<std::string>>> source,
      int producerId,
      int itemIndex = 0) {
    const int kItemsPerProducer = 5;
    ContinueFuture future;
    if (itemIndex >= kItemsPerProducer) {
      if (producersFinished_.fetch_add(1) + 1 == numProducers_) {
        LOG(ERROR) << "[StringProducer " << producerId
                   << "] Finished. Sending final end signal.";
        source->enqueue(nullptr, &future);
      } else {
        LOG(ERROR) << "[StringProducer " << producerId << "] Finished.";
      }
      return;
    }
    const int globalIndex = producerId * kItemsPerProducer + itemIndex;
    const auto data =
        std::make_shared<std::string>("Message " + std::to_string(globalIndex));
    LOG(ERROR) << "[StringProducer " << producerId << "] Enqueuing: " << *data;
    const auto blockingReason = source->enqueue(data, &future);
    if (blockingReason == BlockingReason::kNotBlocked) {
      produceAsync(source, producerId, itemIndex + 1);
    } else {
      std::move(future).via(executor_).thenValue(
          [this, source, producerId, itemIndex](folly::Unit) {
            LOG(ERROR) << "[StringProducer " << producerId
                       << "] Resuming production...";
            produceAsync(source, producerId, itemIndex + 1);
          });
    }
  }

  void consumeAsync(
      std::shared_ptr<GenericSource<std::shared_ptr<std::string>>> source,
      folly::Promise<folly::Unit>& testComplete,
      int consumerId) {
    ContinueFuture future;
    std::shared_ptr<std::string> data;
    const auto blockingReason = source->next(data, &future);
    if (blockingReason == BlockingReason::kNotBlocked) {
      if (!data) {
        LOG(ERROR) << "[StringConsumer " << consumerId
                   << "] Finished. Decrementing counter.";
        // The last consumer to finish signals test completion.
        if (consumersFinished_.fetch_sub(1) == 1) {
          LOG(ERROR) << "[StringConsumer " << consumerId
                     << "] I am the last one. Test complete.";
          testComplete.setValue();
        }
        return;
      }
      LOG(ERROR) << "[StringConsumer " << consumerId << "] Received: " << *data;
      consumeAsync(source, testComplete, consumerId);
    } else {
      std::move(future).via(executor_).thenValue(
          [this, source, &testComplete, consumerId](folly::Unit) {
            LOG(ERROR) << "[StringConsumer " << consumerId
                       << "] Resuming consumption...";
            consumeAsync(source, testComplete, consumerId);
          });
    }
  }
  folly::Executor* const executor_;
  const int numProducers_;
  const int numConsumers_;

  std::atomic<int> producersFinished_;
  std::atomic<int> consumersFinished_;
};

/// Encapsulates the asynchronous test for GenericSource with RowVectorPtr.
class RowVectorTest {
 public:
  RowVectorTest(
      memory::MemoryPool* pool = nullptr,
      folly::Executor* executor = nullptr,
      int numProducers = 1,
      int numConsumers = 1)
      : pool_(pool),
        executor_(executor),
        numProducers_(numProducers),
        numConsumers_(numConsumers),
        producersFinished_(0),
        consumersFinished_(numConsumers) {}
  void run() {
    LOG(ERROR) << "\n--- Running Async RowVector Test (" << numProducers_
               << "P, " << numConsumers_ << "C) ---";
    const auto source =
        createLocalGenericSource<RowVectorPtr>("RowVectorTestSource", 3);
    folly::Promise<folly::Unit> testComplete;
    auto testCompleteFuture = testComplete.getFuture();
    for (int i = 0; i < numConsumers_; ++i) {
      consumeAsync(source, testComplete, i);
    }
    for (int i = 0; i < numProducers_; ++i) {
      produceAsync(source, i);
    }
    testCompleteFuture.wait();
    LOG(ERROR) << "--- Async RowVector Test Finished ---\n";
  }

 private:
  void produceAsync(
      std::shared_ptr<GenericSource<RowVectorPtr>> source,
      int producerId,
      int itemIndex = 0) {
    constexpr auto kItemsPerProducer = 5;
    ContinueFuture future;
    if (itemIndex >= kItemsPerProducer) {
      if (producersFinished_.fetch_add(1) + 1 == numProducers_) {
        LOG(ERROR) << "[RowVectorProducer " << producerId
                   << "] Finished. Sending final end signal.";
        source->enqueue(nullptr, &future);
      } else {
        LOG(ERROR) << "[RowVectorProducer " << producerId << "] Finished.";
      }
      return;
    }
    VectorFuzzer::Options opts;
    opts.vectorSize = 10;
    VectorFuzzer fuzzer(opts, pool_);
    const auto rowType = ROW({{"c0", BIGINT()}, {"c1", VARCHAR()}});
    const auto data = fuzzer.fuzzInputRow(rowType);
    LOG(ERROR) << "[RowVectorProducer " << producerId
               << "] Enqueuing a RowVector of size " << data->size();
    const auto blockingReason = source->enqueue(data, &future);
    if (blockingReason == BlockingReason::kNotBlocked) {
      produceAsync(source, producerId, itemIndex + 1);
    } else {
      std::move(future).via(executor_).thenValue(
          [this, source, producerId, itemIndex](folly::Unit) {
            LOG(ERROR) << "[RowVectorProducer " << producerId
                       << "] Resuming production...";
            produceAsync(source, producerId, itemIndex + 1);
          });
    }
  }
  void consumeAsync(
      std::shared_ptr<GenericSource<RowVectorPtr>> source,
      folly::Promise<folly::Unit>& testComplete,
      int consumerId) {
    ContinueFuture future;
    RowVectorPtr data;
    const auto blockingReason = source->next(data, &future);
    if (blockingReason == BlockingReason::kNotBlocked) {
      if (!data) {
        LOG(ERROR) << "[RowVectorConsumer " << consumerId
                   << "] Finished. Decrementing counter.";
        if (consumersFinished_.fetch_sub(1) == 1) {
          LOG(ERROR) << "[RowVectorConsumer " << consumerId
                     << "] I am the last one. Test complete.";
          testComplete.setValue();
        }
        return;
      }
      LOG(ERROR) << "[RowVectorConsumer " << consumerId
                 << "] Received RowVector with " << data->size()
                 << " rows. First row: " << data->toString(0);
      consumeAsync(source, testComplete, consumerId);
    } else {
      std::move(future).via(executor_).thenValue(
          [this, source, &testComplete, consumerId](folly::Unit) {
            LOG(ERROR) << "[RowVectorConsumer " << consumerId
                       << "] Resuming consumption...";
            consumeAsync(source, testComplete, consumerId);
          });
    }
  }

  memory::MemoryPool* const pool_;
  folly::Executor* const executor_;
  const int numProducers_;
  const int numConsumers_;

  std::atomic<int> producersFinished_;
  std::atomic<int> consumersFinished_;
};
} // namespace boltrain

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  auto pool = memory::memoryManager()->addLeafPool();
  // Set up a shared executor for all async tests.
  folly::CPUThreadPoolExecutor executor(
      std::thread::hardware_concurrency(),
      std::make_shared<folly::NamedThreadFactory>("TestExecutor"));

  boltrain::SimpleStringTest simpleTest(&executor);
  simpleTest.run();

  boltrain::StringTest stringTest(&executor, 2, 2);
  stringTest.run();

  boltrain::RowVectorTest rowVectorTest(pool.get(), &executor);
  rowVectorTest.run();
  return 0;
}
