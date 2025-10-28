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
/*
 * Simplified and optimized Left Merge Join under strict input guarantees.
 *
 * Assumptions (MUST hold):
 *  - Left: within a RowVector, all rows share the same join key; across
 * RowVectors, keys strictly increase.
 *  - Right: join keys are globally unique and strictly increasing; within a
 * RowVector, keys are distinct and increasing.
 *  - Join keys never NULL on both sides.
 *  - Join type: LEFT ONLY.
 *  - Output per call strictly equals current left batch size (dynamic
 * capacity).
 *
 * Key optimization:
 *  - When matched (R == L): right projections are Constant-encoded
 * (wrapInConstant) referencing the single matching right row (zero-copy).
 *  - When unmatched (R > L or right exhausted): right projections are constant
 * NULL.
 */

#include "velox/exec/LeftMergeJoin.h"

#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

namespace {

// Build a single-element null base vector for a given type (used for R > L or
// right exhausted).
VectorPtr makeSingleNullBase(const TypePtr& type, memory::MemoryPool* pool) {
  auto base = BaseVector::create(type, 1, pool);
  base->setNull(0, true);
  return base;
}

} // namespace

LeftMergeJoin::LeftMergeJoin(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::MergeJoinNode>& joinNode)
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
    if (auto outIdx = outputType_->getChildIdxIfExists(name)) {
      leftProjections_.emplace_back(i, outIdx.value());
    }
  }
  for (auto i = 0; i < rightType->size(); ++i) {
    const auto name = rightType->nameOf(i);
    if (auto outIdx = outputType_->getChildIdxIfExists(name)) {
      rightProjections_.emplace_back(i, outIdx.value());
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
  // strictly increase.
  loadColumns(input_, *operatorCtx_->execCtx());
  leftRowIndex_ = 0;
}

// >0 => left > right (R < L); <0 => left < right (R > L)
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

  const vector_size_t capacity = left->size();
  VELOX_CHECK_GT(capacity, 0, "Left batch must be non-empty.");

  // Left dictionary indices.
  leftOutputIndices_ = allocateIndices(capacity, pool());
  rawLeftOutputIndices_ = leftOutputIndices_->asMutable<vector_size_t>();

  std::vector<VectorPtr> columns(outputType_->size());

  // Left columns: dictionary-wrapped from 'left'.
  for (const auto& proj : leftProjections_) {
    columns[proj.outputChannel] = BaseVector::wrapInDictionary(
        {}, leftOutputIndices_, capacity, left->childAt(proj.inputChannel));
  }
  currentLeft_ = left;

  // Right columns:
  for (const auto& proj : rightProjections_) {
    const auto outType = outputType_->childAt(proj.outputChannel);
    if (matched) {
      VELOX_CHECK_NOT_NULL(right);
      VELOX_CHECK_LT(rightRowIndex, right->size());
      auto child = right->childAt(proj.inputChannel);
      columns[proj.outputChannel] =
          BaseVector::wrapInConstant(capacity, rightRowIndex, child);
    } else {
      auto nullBase = makeSingleNullBase(outType, operatorCtx_->pool());
      columns[proj.outputChannel] =
          BaseVector::wrapInConstant(capacity, 0, nullBase);
    }
  }
  currentRight_ = right;

  output_ = std::make_shared<RowVector>(
      operatorCtx_->pool(), outputType_, nullptr, capacity, std::move(columns));
  outputSize_ = 0;
  return false;
}

bool LeftMergeJoin::tryAddLeftIndex(vector_size_t leftRow) {
  VELOX_CHECK(output_);
  const auto capacity = output_->size();
  VELOX_CHECK_LT(outputSize_, capacity);

  rawLeftOutputIndices_[outputSize_] = leftRow;
  ++outputSize_;
  return true;
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
    const auto br =
        rightSource_->next(&rightSideInputFuture_, &rightInput_, drained);
    if (br != BlockingReason::kNotBlocked) {
      return false; // blocked on right
    }
    if (!rightInput_) {
      noMoreRightInput_ = true;
    } else {
      // No NULL filtering: keys guaranteed non-null by contract.
      rightRowIndex_ = 0;
    }
  }
  return true;
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

    const auto leftSize = input_->size();
    while (leftRowIndex_ < leftSize) {
      tryAddLeftIndex(leftRowIndex_);
      ++leftRowIndex_;
    }

    clearLeftInput();
    return produceOutput();
  }

  // Need right input? let outer getOutput() pull it.
  if (!rightInput_) {
    return nullptr;
  }

  // Left batch key: use row 0 (all rows share the same key). No NULL check by
  // contract.
  const auto leftKeyRow = 0;

  // Advance right cursor until rightKey >= leftKey.
  int32_t cmp = 0;
  while (true) {
    // rightInput_ must exist here; if consumed, outer loop pulls next batch.
    cmp = compareKey(
        leftKeyChannels_,
        input_,
        leftKeyRow,
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
    // R > L
    prepareOutputConstantRight(input_, /*right*/ nullptr, /*matched*/ false, 0);
  }

  // Fill left indices for the entire left batch.
  const auto leftSize = input_->size();
  while (leftRowIndex_ < leftSize) {
    tryAddLeftIndex(leftRowIndex_);
    ++leftRowIndex_;
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
    auto out = doGetOutput();
    if (out && out->size() > 0) {
      return out; // guaranteed out->size() == left->size()
    }

    // Pull right input if needed.
    if (needsInputFromRightSide()) {
      if (!getNextFromRightSide()) {
        return nullptr; // blocked on right
      }
      continue; // loop to attempt production again
    }

    return nullptr;
  }
  VELOX_UNREACHABLE();
}

bool LeftMergeJoin::isFinished() {
  // Finish when both sides are at end and no current batches are pending.
  return noMoreInput_ && input_ == nullptr && noMoreRightInput_ &&
      rightInput_ == nullptr;
}

void LeftMergeJoin::close() {
  if (rightSource_) {
    rightSource_->close();
  }
  Operator::close();
}

} // namespace facebook::velox::exec
