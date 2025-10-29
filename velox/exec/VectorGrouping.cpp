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

#include "velox/exec/VectorGrouping.h"

#include "OperatorUtils.h"

namespace facebook::velox::exec {
bool VectorGrouping::equal(
    const RowVectorPtr& vector,
    vector_size_t lhs,
    vector_size_t rhs) const {
  for (const auto key : keyChannels_) {
    const auto result =
        vector->childAt(key)->compare(vector->childAt(key).get(), lhs, rhs);
    if (result != 0) {
      return false;
    }
  }
  return true;
}

bool VectorGrouping::equal(
    const RowVectorPtr& vector,
    vector_size_t index,
    const RowVectorPtr& other,
    vector_size_t otherIndex) const {
  for (const auto key : keyChannels_) {
    const auto result = vector->childAt(key)->compare(
        other->childAt(key).get(), index, otherIndex);
    if (result != 0) {
      return false;
    }
  }
  return true;
}

VectorGrouping::VectorGrouping(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::VectorGroupingNode>& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "VectorGrouping"),
      maxInputHolded_(node->maxInputHolded()) {
  for (const auto& key : node->keys()) {
    keyChannels_.push_back(outputType_->getChildIdx(key->name()));
  }
}

void VectorGrouping::addInput(RowVectorPtr input) {
  loadColumns(input, *operatorCtx_->execCtx());
  std::vector<RowVectorPtr> slices;
  const auto numRows = input->size();
  VELOX_CHECK_GT(numRows, 0);

  vector_size_t lastStart = 0;
  vector_size_t pos = lastStart + 1;

  for (; pos < numRows; ++pos) {
    if (!equal(input, lastStart, pos)) {
      // Create a slice for the range [lastStart, pos).
      slices.push_back(
          std::static_pointer_cast<RowVector>(
              input->slice(lastStart, pos - lastStart)));
      lastStart = pos;
    }
  }

  // Final slice for the range [lastStart, numRows).
  if (FOLLY_UNLIKELY(lastStart == 0)) {
    // Entire batch is a single run, reuse the original input.
    slices.push_back(input);
  } else {
    VELOX_CHECK_LT(lastStart, numRows);
    slices.push_back(
        std::static_pointer_cast<RowVector>(
            input->slice(lastStart, numRows - lastStart)));
  }

  VELOX_CHECK_GT(slices.size(), 0);
  if (FOLLY_UNLIKELY(inputs_.empty())) {
    for (auto& slice : slices) {
      inputs_.push_back(slice);
    }
    return;
  }

  auto& lastInput = inputs_.back();
  size_t index = 0;
  if (equal(lastInput, 0, slices[0], 0)) {
    lastInput->append(slices[0].get());
    ++index;
  }
  for (; index < slices.size(); ++index) {
    inputs_.push_back(std::move(slices[index]));
  }
}

bool VectorGrouping::needsInput() const {
  return !noMoreInput_ && inputs_.size() < maxInputHolded_;
}

// If there’s only one buffered slice, emit it only after `noMoreInput_` is
// true. This avoids emitting a slice that might be extended by the next batch
// (same key).
RowVectorPtr VectorGrouping::getOutput() {
  if (inputs_.empty()) {
    return nullptr;
  }

  if (inputs_.size() == 1) {
    if (noMoreInput_) {
      auto output = inputs_.front();
      inputs_.pop_front();
      return output;
    }
    return nullptr;
  }

  // When there are >= 2 slices buffered, the first slice's key boundary is
  // closed by the presence of the next slice, so it's safe to emit now.
  auto output = inputs_.front();
  inputs_.pop_front();
  return output;
}
} // namespace facebook::velox::exec
