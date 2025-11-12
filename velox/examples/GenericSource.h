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
#pragma once

#include <boost/circular_buffer.hpp>
#include <folly/Synchronized.h>
#include <velox/common/base/Exceptions.h>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include "velox/exec/BlockingReason.h"
#include "velox/vector/ComplexVector.h"

namespace boltrain {

namespace Velox = facebook::velox;
namespace VeloxExec = facebook::velox::exec;

/// A generic, thread-safe, producer-consumer data source interface.
///
/// @tparam T The data type stored in the queue. This type must be
///           constructible from nullptr to support the end-of-stream signal.
template <typename T>
class GenericSource {
 public:
  virtual ~GenericSource() = default;

  /// Retrieves the next data item from the queue.
  /// @param data An output parameter to receive the data item.
  /// @param future If the operation is blocked, it will be resumed via this
  /// future.
  /// @return A blocking reason indicating if the operation was blocked and why.
  virtual VeloxExec::BlockingReason next(
      T& data,
      Velox::ContinueFuture* future) = 0;

  /// Adds a data item to the queue.
  /// @param input The data item to add. Passing a null value (e.g., nullptr)
  ///              signals the end of the stream.
  /// @param future If the operation is blocked, it will be resumed via this
  /// future.
  /// @return A blocking reason indicating if the operation was blocked and why.
  virtual VeloxExec::BlockingReason enqueue(
      T input,
      Velox::ContinueFuture* future) = 0;

  /// Returns the name of the data source.
  virtual std::string name() = 0;
};

/// A helper class to automatically fulfill promises upon scope exit.
class ScopedPromiseNotification {
 public:
  explicit ScopedPromiseNotification(size_t initSize) {
    promises_.reserve(initSize);
  }

  ~ScopedPromiseNotification() {
    for (auto& promise : promises_) {
      promise.setValue();
    }
  }

  void add(std::vector<Velox::ContinuePromise>&& promises) {
    promises_.reserve(promises_.size() + promises.size());
    for (auto& promise : promises) {
      promises_.emplace_back(std::move(promise));
    }
    promises.clear();
  }

  void add(Velox::ContinuePromise&& promise) {
    promises_.emplace_back(std::move(promise));
  }

 private:
  std::vector<Velox::ContinuePromise> promises_;
};

/// The core logic implementation of a thread-safe MPMC queue.
template <typename T>
class GenericSourceQueue {
 public:
  explicit GenericSourceQueue(int queueSize) : data_(queueSize) {}

  VeloxExec::BlockingReason next(
      T& data,
      Velox::ContinueFuture* future,
      ScopedPromiseNotification& notification) {
    data = T{}; // Reset
    if (data_.empty()) {
      if (atEnd_) {
        return VeloxExec::BlockingReason::kNotBlocked;
      }
      consumerPromises_.emplace_back("GenericSourceQueue::next");
      *future = consumerPromises_.back().getSemiFuture();
      return VeloxExec::BlockingReason::kWaitForProducer;
    }

    data = std::move(data_.front());
    data_.pop_front();
    notifyProducers(notification);
    return VeloxExec::BlockingReason::kNotBlocked;
  }

  VeloxExec::BlockingReason enqueue(
      T input,
      Velox::ContinueFuture* future,
      ScopedPromiseNotification& notification) {
    if (!input) {
      atEnd_ = true;
      notifyConsumers(notification);
      return VeloxExec::BlockingReason::kNotBlocked;
    }

    if (data_.full()) {
      producerPromises_.emplace_back("GenericSourceQueue::enqueue (full)");
      *future = producerPromises_.back().getSemiFuture();
      return VeloxExec::BlockingReason::kWaitForConsumer;
    }

    data_.push_back(std::move(input));
    notifyConsumers(notification);
    return VeloxExec::BlockingReason::kNotBlocked;
  }

 private:
  void notifyConsumers(ScopedPromiseNotification& notification) {
    notification.add(std::move(consumerPromises_));
    VELOX_CHECK(consumerPromises_.empty());
  }

  void notifyProducers(ScopedPromiseNotification& notification) {
    notification.add(std::move(producerPromises_));
    VELOX_CHECK(producerPromises_.empty());
  }

  bool atEnd_{false};
  boost::circular_buffer<T> data_;
  std::vector<Velox::ContinuePromise> consumerPromises_;
  std::vector<Velox::ContinuePromise> producerPromises_;
};

/// Implementation of the GenericSource interface, providing thread-safety via
/// folly::Synchronized.
template <typename T>
class LocalGenericSource final : public GenericSource<T> {
 public:
  // Compile-time check to ensure the template parameter T is "nullable".
  static_assert(
      std::is_constructible_v<T, std::nullptr_t>,
      "Template parameter T must be constructible from nullptr "
      "to support the end-of-stream signal.");

  explicit LocalGenericSource(const std::string& name, int queueSize)
      : name_(name), queue_(GenericSourceQueue<T>(queueSize)) {}

  VeloxExec::BlockingReason next(T& data, Velox::ContinueFuture* future)
      override {
    ScopedPromiseNotification notification(1);
    return queue_.withWLock(
        [&](auto& queue) { return queue.next(data, future, notification); });
  }

  VeloxExec::BlockingReason enqueue(T input, Velox::ContinueFuture* future)
      override {
    ScopedPromiseNotification notification(1);
    return queue_.withWLock([&](auto& queue) {
      return queue.enqueue(std::move(input), future, notification);
    });
  }

  std::string name() override {
    return name_;
  }

 private:
  const std::string name_;
  folly::Synchronized<GenericSourceQueue<T>> queue_;
};

/// Creates a local memory-based and thread-safe instance of GenericSource.
///
/// @tparam T The data type to be stored in the queue.
/// @param name Name of the source.
/// @param queueSize The maximum capacity of the internal circular buffer.
/// @return A shared pointer to the GenericSource interface.
template <typename T>
std::shared_ptr<GenericSource<T>> createLocalGenericSource(
    const std::string& name,
    int queueSize) {
  return std::make_shared<LocalGenericSource<T>>(name, queueSize);
}

} // namespace boltrain
