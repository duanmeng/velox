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

/// VectorGrouping is a streaming Velox Operator that groups consecutive rows by
/// a set of key columns and produces columnar outputs where each output vector
/// contains rows for exactly one key (e.g., one uid).
///
/// - Input ordering assumption: upstream guarantees rows are globally sorted by
///   the grouping keys. Identical keys may span multiple consecutive input
///   batches.
/// - Columnar-only: it does not convert columns to row containers; outputs are
///   built via slicing/concatenation to keep zero-copy/low-copy semantics.
/// - Slicing: each input RowVector is partitioned into contiguous slices where
///   all rows share the same key. Adjacent slices form boundaries when a key
///   changes.
/// - Cross-batch group: if the last buffered slice and the first slice of the
///   new batch have the same key, they are appended to form a single output
////   vector for that key (preventing fragmentation across batches).
/// - Backpressure: because each input batch may produce multiple slices but
///   getOutput() emits only one slice per call, needsInput() throttles the
///   source when the internal buffer grows (simple watermark).
/// - Emission rule: the last (in-progress) slice is held until end-of-input,
///   ensuring an output vector contains all rows for its key.

#pragma once

#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
class VectorGrouping : public Operator {
 public:
  VectorGrouping(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::VectorGroupingNode>& node);

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  /// Backpressure the source because each input vector may be sliced into
  /// multiple vectors, but only one vector is emitted per `getOutput` call.
  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override {
    return noMoreInput_ && inputs_.empty();
  }

 private:
  bool equal(const RowVectorPtr& vector, vector_size_t lhs, vector_size_t rhs)
      const;

  bool equal(
      const RowVectorPtr& vector,
      vector_size_t index,
      const RowVectorPtr& other,
      vector_size_t otherIndex) const;

  std::vector<column_index_t> keyChannels_;
  std::list<RowVectorPtr> inputs_;
  const int32_t maxInputHolded_;
};
} // namespace facebook::velox::exec
