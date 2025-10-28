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

#include "KafkaConnector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::memory;
using namespace facebook::velox::common;

namespace boltrain::kafka {
void KafkaDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split_,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<KafkaConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");
  splitEnd_ = split_->numRows;
}

std::optional<RowVectorPtr> KafkaDataSource::next(
    uint64_t size,
    ContinueFuture& future) {
  if (splitOffset_ >= splitEnd_) {
    split_ = nullptr;
    splitOffset_ = 0;
    splitEnd_ = 0;
    return nullptr;
  }

  const size_t outputRows = std::min(size, (splitEnd_ - splitOffset_));
  splitOffset_ += outputRows;

  auto outputVector = vectorFuzzer_->fuzzRow(outputType_, outputRows);
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();
  return outputVector;
}

} // namespace boltrain::kafka
