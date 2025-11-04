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
#include "velox/exec/LeftMergeJoin.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {
namespace {

// Build a single-element null base vector for a given type (used for R > L or
// right exhausted).
VectorPtr makeSingleNullBase(const TypePtr& type, memory::MemoryPool* pool) {
  const auto base = BaseVector::create(type, 1, pool);
  base->setNull(0, true);
  return base;
}

VectorPtr makeSingleValueBase(
    const VectorPtr& child,
    vector_size_t sourceIndex,
    memory::MemoryPool* pool) {
  const auto type = child->type();
  const auto base = BaseVector::create(type, 1, pool);
  base->copy(child.get(), 0, sourceIndex, 1);
  return base;
}

} // namespace

LeftMergeJoin::LeftMergeJoin(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::LeftMergeJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "LeftMergeJoin"),
      numKeys_{joinNode->leftKeys().size()},
      rightNodeId_{joinNode->sources()[1]->id()},
      joinNode_(joinNode) {
  VELOX_CHECK(joinNode->isLeftJoin(), "LeftMergeJoin supports only LEFT JOIN");
}

void LeftMergeJoin::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(joinNode_);

  const auto& leftType = joinNode_->sources()[0]->outputType();
  const auto& rightType = joinNode_->sources()[1]->outputType();

  leftKeyChannels_.reserve(numKeys_);
  rightKeyChannels_.reserve(numKeys_);

  for (const auto& key : joinNode_->leftKeys()) {
    leftKeyChannels_.push_back(leftType->getChildIdx(key->name()));
  }
  for (const auto& key : joinNode_->rightKeys()) {
    rightKeyChannels_.push_back(rightType->getChildIdx(key->name()));
  }

  // Identity projections: align by output names.
  for (auto i = 0; i < leftType->size(); ++i) {
    const auto name = leftType->nameOf(i);
    if (const auto index = outputType_->getChildIdxIfExists(name)) {
      leftProjections_.emplace_back(i, index.value());
    }
  }
  for (auto i = 0; i < rightType->size(); ++i) {
    const auto name = rightType->nameOf(i);
    if (const auto index = outputType_->getChildIdxIfExists(name)) {
      rightProjections_.emplace_back(i, index.value());
    }
  }

  joinNode_.reset();
}

BlockingReason LeftMergeJoin::isBlocked(ContinueFuture* future) {
  if (rightSideInputFuture_.valid()) {
    *future = std::move(rightSideInputFuture_);
    return BlockingReason::kWaitForMergeJoinRightSide;
  }
  return BlockingReason::kNotBlocked;
}

bool LeftMergeJoin::needsInput() const {
  return input_ == nullptr;
}

void LeftMergeJoin::addInput(RowVectorPtr input) {
  VELOX_CHECK_NULL(input_);
  VELOX_CHECK_NOT_NULL(input);

  input_ = std::move(input);

  // Left batch: per contract, all rows share the same key; across batches keys
  // strictly increase. Also ensure columns are materialized (no lazy).
  loadColumns(input_, *operatorCtx_->execCtx());
  leftRowIndex_ = 0;
}

int32_t LeftMergeJoin::compareKey(
    const std::vector<column_index_t>& keyChannels,
    const RowVectorPtr& batch,
    vector_size_t index,
    const std::vector<column_index_t>& otherKeyChannels,
    const RowVectorPtr& otherBatch,
    vector_size_t otherIndex) {
  const auto n = keyChannels.size();
  for (size_t i = 0; i < n; ++i) {
    const auto cmp = batch->childAt(keyChannels[i])
                         ->compare(
                             otherBatch->childAt(otherKeyChannels[i]).get(),
                             index,
                             otherIndex,
                             CompareFlags{});
    VELOX_CHECK(cmp.has_value());
    if (cmp.value() != 0) {
      return cmp.value();
    }
  }
  return 0;
}

bool LeftMergeJoin::prepareOutputConstantRight(
    const RowVectorPtr& left,
    const RowVectorPtr& right,
    bool matched,
    vector_size_t rightRowIndex) {
  VELOX_CHECK_NULL(
      output_, "prepareOutputConstantRight must start a fresh output.");
  VELOX_CHECK_NOT_NULL(left);

  // Ensure left columns are materialized before sharing into output.
  loadColumns(left, *operatorCtx_->execCtx());

  const vector_size_t numLeftSizeRows = left->size();
  VELOX_CHECK_GT(numLeftSizeRows, 0, "Left batch must be non-empty.");

  std::vector<VectorPtr> columns(outputType_->size());

  // Left columns: directly reuse left children.
  for (const auto& projects : leftProjections_) {
    columns[projects.outputChannel] = left->childAt(projects.inputChannel);
  }
  currentLeft_ = left;

  // Right columns (copy-based: materialize single-element base, then wrap
  // constant).
  if (FOLLY_LIKELY(matched)) {
    VELOX_CHECK_NOT_NULL(right);
    VELOX_CHECK_LT(rightRowIndex, right->size());
    for (const auto& projects : rightProjections_) {
      const auto& child = right->childAt(projects.inputChannel);
      columns[projects.outputChannel] =
          BaseVector::wrapInConstant(numLeftSizeRows, rightRowIndex, child);
    }
  } else {
    for (const auto& projects : rightProjections_) {
      const auto outType = outputType_->childAt(projects.outputChannel);
      const auto nullBase = makeSingleNullBase(outType, pool());
      columns[projects.outputChannel] =
          BaseVector::wrapInConstant(numLeftSizeRows, 0, nullBase);
    }
  }

  currentRight_ = right;
  output_ = std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numLeftSizeRows, std::move(columns));

  // No index filling needed when reusing left columns directly.
  outputSize_ = numLeftSizeRows;
  return false;
}

bool LeftMergeJoin::needsInputFromRightSide() const {
  return !noMoreRightInput_ && !rightSideInputFuture_.valid() && !rightInput_;
}

bool LeftMergeJoin::getNextFromRightSide() {
  VELOX_CHECK(needsInputFromRightSide());
  if (!rightSource_) {
    rightSource_ = operatorCtx_->task()->getMergeJoinSource(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  }

  while (!noMoreRightInput_ && !rightInput_) {
    bool drained = false;
    const auto blockingReason =
        rightSource_->next(&rightSideInputFuture_, &rightInput_, drained);
    if (blockingReason != BlockingReason::kNotBlocked) {
      VELOX_CHECK_NULL(rightInput_);
      VELOX_CHECK(rightSideInputFuture_.valid());
      return false;
    }
    if (!rightInput_) {
      noMoreRightInput_ = true;
    } else {
      rightRowIndex_ = 0;
    }
  }
  return true;
}

RowVectorPtr LeftMergeJoin::produceOutput() {
  VELOX_CHECK_NOT_NULL(output_);
  output_->resize(outputSize_);
  return std::move(output_);
}

RowVectorPtr LeftMergeJoin::doGetOutput() {
  // If no current left batch, nothing to produce.
  if (!input_) {
    return nullptr;
  }

  // If right exhausted: produce left-only with constant NULL right.
  if (!rightInput_ && noMoreRightInput_) {
    VELOX_CHECK_NULL(output_);
    prepareOutputConstantRight(input_, /*right*/ nullptr, /*matched*/ false, 0);

    // Left columns are directly reused; output already full capacity.
    clearLeftInput();
    return produceOutput();
  }

  // Need right input? let outer getOutput() pull it.
  if (!rightInput_) {
    return nullptr;
  }

  // Left batch keys are the same across all rows in a batch.
  constexpr vector_size_t kLeftKeyRows = 0;
  // Advance right cursor until rightKey >= leftKey.
  int32_t cmp = 0;
  while (true) {
    // rightInput_ must exist here; if consumed, outer loop pulls next batch.
    cmp = compareKey(
        leftKeyChannels_,
        input_,
        kLeftKeyRows,
        rightKeyChannels_,
        rightInput_,
        rightRowIndex_);
    if (cmp > 0) { // left > right => R < L: advance right
      ++rightRowIndex_;
      if (rightRowIndex_ >= rightInput_->size()) {
        clearRightInput();
        return nullptr;
      }
      continue;
    }
    // cmp <= 0 => R >= L
    break;
  }

  const bool matched = (cmp == 0);

  // Prepare output (right as constant values or constant NULL).
  VELOX_CHECK_NULL(output_);
  if (matched) {
    prepareOutputConstantRight(
        input_, rightInput_, /*matched*/ true, rightRowIndex_);
  } else {
    // R > L, output the left batch directly.
    prepareOutputConstantRight(input_, /*right*/ nullptr, /*matched*/ false, 0);
  }

  // Post actions:
  if (matched) {
    // Right key unique: advance right cursor once.
    ++rightRowIndex_;
    if (rightRowIndex_ >= rightInput_->size()) {
      clearRightInput();
    }
  } else {
    // R > L: keep right cursor in place.
  }

  clearLeftInput();
  return produceOutput();
}

RowVectorPtr LeftMergeJoin::getOutput() {
  for (;;) {
    const auto output = doGetOutput();
    if (output != nullptr) {
      VELOX_CHECK_GT(output->size(), 0);
      return output;
    }

    // Pull right input if needed.
    if (needsInputFromRightSide()) {
      if (!getNextFromRightSide()) {
        return nullptr; // blocked on right
      }
      continue;
    }

    return nullptr;
  }
  VELOX_UNREACHABLE();
}

bool LeftMergeJoin::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}

void LeftMergeJoin::close() {
  if (rightSource_) {
    rightSource_->close();
  }
  rightInput_ = nullptr;
  noMoreRightInput_ = true;
  Operator::close();
}

} // namespace facebook::velox::exec
