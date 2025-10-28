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

#include "velox/exec/MergeSource.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
/// Simplified and optimized Left Merge Join under strict input guarantees.
///
/// Assumptions (MUST hold):
///  - Left: within a RowVector, all rows share the same join key; across
///    RowVectors, keys strictly increase.
///  - Right: join keys are globally unique and strictly increasing; within a
///    RowVector, keys are distinct and increasing.
///  - Join keys never NULL on both sides.
///  - Join type: LEFT ONLY.
///  - Output per call strictly equals current left batch size (dynamic
///    capacity).
///
/// Key optimization:
///  - When matched (R == L): right projections are Constant-encoded
///    (wrapInConstant) referencing the single matching right row (zero-copy).
///  - When unmatched (R > L or right exhausted): right projections are constant
///    NULL.
class LeftMergeJoin : public Operator {
 public:
  LeftMergeJoin(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::MergeJoinNode>& joinNode);

  void initialize() override;
  BlockingReason isBlocked(ContinueFuture* future) override;

  bool needsInput() const override;
  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override;

  void close() override;

 private:
  // Compare join key at (batch,index) vs (otherBatch,otherIndex).
  // equalsOnly, no NULLs by contract.
  static int32_t compareKey(
      const std::vector<column_index_t>& keyChannels,
      const RowVectorPtr& batch,
      vector_size_t index,
      const std::vector<column_index_t>& otherKeyChannels,
      const RowVectorPtr& otherBatch,
      vector_size_t otherIndex);

  // Build output with capacity == left->size():
  //  - Left: directly reuse child vectors from 'left' (no dictionary wrapping).
  //  - Right: constant columns:
  //      matched   => wrapInConstant(capacity, rightRowIndex, right child)
  //      unmatched => wrapInConstant(capacity, 0, single-null base)
  // Must be called only when starting a fresh output
  // (output_ == nullptr) and left != nullptr.
  bool prepareOutputConstantRight(
      const RowVectorPtr& left,
      const RowVectorPtr& right,
      bool matched,
      vector_size_t rightRowIndex);

  RowVectorPtr produceOutput();

  // Right side feed.
  bool needsInputFromRightSide() const;
  bool getNextFromRightSide();

  // Main output routine: produce one full-batch output for the current left
  // batch.
  RowVectorPtr doGetOutput();

  void clearLeftInput() {
    input_ = nullptr;
    leftRowIndex_ = 0;
  }

  void clearRightInput() {
    rightInput_ = nullptr;
    rightRowIndex_ = 0;
  }

  // Number of join keys.
  const size_t numKeys_;

  // Right side plan node id.
  const core::PlanNodeId rightNodeId_;

  // Cached node for initialization (consumed in initialize()).
  std::shared_ptr<const core::MergeJoinNode> joinNode_;

  // Key channels and projections.
  std::vector<column_index_t> leftKeyChannels_;
  std::vector<column_index_t> rightKeyChannels_;
  std::vector<IdentityProjection> leftProjections_;
  std::vector<IdentityProjection> rightProjections_;

  // Current base batches used when building output.
  RowVectorPtr currentLeft_;
  RowVectorPtr currentRight_;

  // Right side source and current batch.
  std::shared_ptr<MergeJoinSource> rightSource_;
  RowVectorPtr rightInput_;
  vector_size_t rightRowIndex_{0};

  // Current left batch and index.
  RowVectorPtr input_;
  vector_size_t leftRowIndex_{0};

  // Output buffer; capacity == current left batch size.
  RowVectorPtr output_;
  vector_size_t outputSize_{0};

  // Blocking future for right side pulling.
  ContinueFuture rightSideInputFuture_{ContinueFuture::makeEmpty()};
  bool noMoreRightInput_{false};
};

} // namespace facebook::velox::exec
