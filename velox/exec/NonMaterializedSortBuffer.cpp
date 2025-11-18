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

#include "velox/exec/NonMaterializedSortBuffer.h"

#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/Spiller.h"
#include "velox/expression/VectorReaders.h"

namespace facebook::velox::exec {

NonMaterializedSortBuffer::NonMaterializedSortBuffer(
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& sortColumnIndices,
    const std::vector<CompareFlags>& sortCompareFlags,
    velox::memory::MemoryPool* pool,
    tsan_atomic<bool>* nonReclaimableSection,
    common::PrefixSortConfig prefixSortConfig,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<velox::common::SpillStats>* spillStats)
    : SortBufferBase(
          inputType,
          sortColumnIndices,
          sortCompareFlags,
          pool,
          nonReclaimableSection,
          prefixSortConfig,
          spillConfig,
          spillStats),
      sortingKeys_(
          SpillState::makeSortingKeys(sortColumnIndices, sortCompareFlags)) {
  // Sorted key columns.
  std::vector<TypePtr> sortedColumnTypes;
  sortedColumnTypes.reserve(sortColumnIndices.size());
  for (column_index_t i = 0; i < sortColumnIndices.size(); ++i) {
    columnMap_.emplace_back(IdentityProjection(i, sortColumnIndices.at(i)));
    sortedColumnTypes.emplace_back(
        inputType_->childAt(sortColumnIndices.at(i)));
  }

  // Vector index and row index columns.
  const auto numSortKeys = columnMap_.size();
  for (auto i = 0; i < indexType_->size(); ++i) {
    indexColumnMap_.emplace_back(numSortKeys + i, i);
  }
  data_ = std::make_unique<RowContainer>(
      sortedColumnTypes, indexType_->children(), pool_);
}

NonMaterializedSortBuffer::~NonMaterializedSortBuffer() {
  inputs_.clear();
  rowIndices_.reset();
  pool_->release();
}

void NonMaterializedSortBuffer::addInput(const VectorPtr& input) {
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::HybridSortBuffer::addInput", this);
  VELOX_CHECK(!noMoreInput_);
  ensureInputFits(input);

  const SelectivityVector allRows(input->size());
  std::vector<char*> rows(input->size());
  for (int row = 0; row < input->size(); ++row) {
    rows[row] = data_->newRow();
  }

  // Stores the sort key columns.
  auto* inputRow = input->asChecked<RowVector>();
  for (const auto& columnProjection : columnMap_) {
    DecodedVector decoded(
        *inputRow->childAt(columnProjection.outputChannel), allRows);
    data_->store(
        decoded,
        folly::Range(rows.data(), input->size()),
        columnProjection.inputChannel);
  }

  // Stores the vector indices column.
  inputs_.push_back(std::static_pointer_cast<RowVector>(input));
  const auto vectorIndex = std::make_shared<ConstantVector<int64_t>>(
      pool(),
      input->size(),
      /*isNull*/ false,
      BIGINT(),
      inputs_.size() - 1);
  DecodedVector decoded;
  decoded.decode(*vectorIndex, allRows);
  const auto numSortKeys = columnMap_.size();
  data_->store(decoded, folly::Range(rows.data(), input->size()), numSortKeys);

  // Stores the row indices column.
  const auto rowIndices =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), input->size(), pool());
  for (int64_t i = 0; i < input->size(); ++i) {
    rowIndices->mutableRawValues()[i] = i;
  }
  decoded.decode(*rowIndices, allRows);
  data_->store(
      decoded, folly::Range(rows.data(), input->size()), numSortKeys + 1);

  numInputRows_ += allRows.size();
  numInputBytes_ += input->retainedSize();
}

void NonMaterializedSortBuffer::noMoreInput() {
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::HybridSortBuffer::noMoreInput", this);
  VELOX_CHECK(!noMoreInput_);
  VELOX_CHECK_NULL(outputSpiller_);
  // It may trigger spill, make sure it's triggered before noMoreInput_ is set.
  ensureSortFits();

  noMoreInput_ = true;

  // No data.
  if (numInputRows_ == 0) {
    return;
  }
  estimatedOutputRowSize_ = numInputBytes_ / numInputRows_;

  if (inputSpiller_ == nullptr) {
    VELOX_CHECK_EQ(numInputRows_, data_->numRows());
    sortInput(numInputRows_);
  } else {
    // Spill the remaining in-memory state to disk if spilling has been
    // triggered on this sort buffer. This is to simplify query OOM prevention
    // when producing output as we don't support to spill during that stage as
    // for now.
    spill();
  }

  // Releases the unused memory reservation after procesing input.
  pool_->release();
}

bool NonMaterializedSortBuffer::hasSpilled() const {
  if (inputSpiller_ != nullptr) {
    VELOX_CHECK_NULL(outputSpiller_);
    return true;
  }
  return outputSpiller_ != nullptr;
}

int64_t NonMaterializedSortBuffer::estimateFlatInputBytes(
    const VectorPtr& input) const {
  int64_t flatInputBytes{0};
  const auto inputRowVector = input->asUnchecked<RowVector>();
  for (const auto identity : columnMap_) {
    flatInputBytes +=
        inputRowVector->childAt(identity.outputChannel)->estimateFlatSize();
  }
  flatInputBytes += indexColumnMap_.size() * sizeof(int64_t) * input->size();
  return flatInputBytes;
}

int64_t NonMaterializedSortBuffer::estimateIncrementalBytes(
    const VectorPtr& input,
    uint64_t outOfLineBytes,
    int64_t flatInputBytes) const {
  return data_->sizeIncrement(
             input->size(), outOfLineBytes ? flatInputBytes : 0) +
      input->retainedSize();
}

std::optional<uint64_t> NonMaterializedSortBuffer::estimateOutputRowSize()
    const {
  return estimatedOutputRowSize_;
}

void NonMaterializedSortBuffer::ensureSortFits() {
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  if (numInputRows_ == 0 || inputSpiller_ != nullptr) {
    return;
  }

  ensureSortFitsImpl();
}

void NonMaterializedSortBuffer::runSpill(
    NoRowContainerSpiller* spiller,
    int64_t numSpillRows,
    uint64_t spillRowOffset) const {
  RowVectorPtr output;
  RowVectorPtr indexOutput;
  int64_t numOutputs{0};
  constexpr int32_t kTargetBatchRows = 64;
  while (numOutputs < numSpillRows) {
    const auto batchSize =
        std::min<int64_t>(kTargetBatchRows, numSpillRows - numOutputs);
    prepareOutputVector(output, inputType_, batchSize);
    prepareOutputVector(indexOutput, indexType_, batchSize);
    gatherCopyOutput(indexOutput, spillRowOffset, output.get());
    VELOX_CHECK_EQ(batchSize, output->size());
    numOutputs += batchSize;
    spillRowOffset += batchSize;
    spiller->spill(SpillPartitionId{0}, output);
  }
  VELOX_CHECK_EQ(numOutputs, numSpillRows);
}

void NonMaterializedSortBuffer::spillInput() {
  VELOX_CHECK_LT(!!(inputSpiller_ == nullptr) + !!noMoreInput_, 2);
  inputSpiller_ = std::make_unique<MergeSpiller>(
      inputType_,
      std::nullopt,
      HashBitRange{},
      sortingKeys_,
      spillConfig_,
      spillStats_);

  sortInput(data_->numRows());
  runSpill(inputSpiller_.get(), data_->numRows(), 0);
  finishInputSpill();
  inputs_.clear();
  data_->clear();
  sortedRows_.clear();
  sortedRows_.shrink_to_fit();
}

void NonMaterializedSortBuffer::spillOutput() {
  if (hasSpilled()) {
    // Already spilled.
    return;
  }
  if (numOutputRows_ == sortedRows_.size()) {
    // All the output has been produced.
    return;
  }

  outputSpiller_ = std::make_unique<NoRowContainerSpiller>(
      inputType_, std::nullopt, HashBitRange{}, spillConfig_, spillStats_);
  runSpill(
      outputSpiller_.get(), numInputRows_ - numOutputRows_, numOutputRows_);
  inputs_.clear();
  data_->clear();
  sortedRows_.clear();
  sortedRows_.shrink_to_fit();
  // Finish right after spilling as the output spiller only spills at most
  // once.
  finishOutputSpill();
}

void NonMaterializedSortBuffer::prepareOutputVector(
    RowVectorPtr& output,
    const RowTypePtr& outputType,
    vector_size_t outputBatchSize) const {
  if (output != nullptr) {
    VectorPtr vector = std::move(output);
    BaseVector::prepareForReuse(vector, outputBatchSize);
    output = std::static_pointer_cast<RowVector>(vector);
  } else {
    output = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType, outputBatchSize, pool_));
  }

  for (const auto& child : output->children()) {
    child->resize(outputBatchSize);
  }
}

void NonMaterializedSortBuffer::prepareOutput(vector_size_t batchSize) {
  prepareOutputVector(output_, inputType_, batchSize);
  prepareOutputVector(indexOutput_, indexType_, batchSize);

  if (hasSpilled()) {
    spillSources_.resize(batchSize);
    spillSourceRows_.resize(batchSize);
    prepareOutputWithSpill();
  }

  VELOX_CHECK_GT(output_->size(), 0);
  VELOX_CHECK_LE(output_->size() + numOutputRows_, numInputRows_);
}

void NonMaterializedSortBuffer::gatherCopyOutput(
    const RowVectorPtr& indexOutput,
    uint64_t sortedRowOffset,
    RowVector* output) const {
  for (const auto& columnProjection : indexColumnMap_) {
    data_->extractColumn(
        sortedRows_.data() + sortedRowOffset,
        indexOutput->size(),
        columnProjection.inputChannel,
        indexOutput->childAt(columnProjection.outputChannel));
  }

  // Extracts vector indices.
  std::vector<const RowVector*> sourceVectors;
  sourceVectors.reserve(indexOutput->size());
  const auto* vectorIndices =
      indexOutput->childAt(0)->asChecked<FlatVector<int64_t>>();
  for (auto i = 0; i < indexOutput->size(); ++i) {
    sourceVectors.push_back(inputs_[vectorIndices->rawValues()[i]].get());
  }

  // Extracts row indices.
  std::vector<vector_size_t> sourceRowIndices;
  sourceRowIndices.reserve(indexOutput->size());
  const auto* rowIndices =
      indexOutput->childAt(1)->asChecked<FlatVector<int64_t>>();
  for (auto i = 0; i < indexOutput->size(); ++i) {
    sourceRowIndices.push_back(rowIndices->rawValues()[i]);
  }

  gatherCopy(output, 0, output->size(), sourceVectors, sourceRowIndices);
}

void NonMaterializedSortBuffer::getOutputWithoutSpill() {
  VELOX_DCHECK_EQ(numInputRows_, sortedRows_.size());
  gatherCopyOutput(indexOutput_, numOutputRows_, output_.get());
  numOutputRows_ += output_->size();
}

void NonMaterializedSortBuffer::getOutputWithSpill() {
  if (spillMerger_ != nullptr) {
    VELOX_DCHECK_EQ(sortedRows_.size(), 0);

    int32_t outputRow = 0;
    int32_t outputSize = 0;
    bool isEndOfBatch = false;
    while (outputRow + outputSize < output_->size()) {
      SpillMergeStream* stream = spillMerger_->next();
      VELOX_CHECK_NOT_NULL(stream);

      spillSources_[outputSize] = &stream->current();
      spillSourceRows_[outputSize] = stream->currentIndex(&isEndOfBatch);
      ++outputSize;
      if (FOLLY_UNLIKELY(isEndOfBatch)) {
        // The stream is at end of input batch. Need to copy out the rows before
        // fetching next batch in 'pop'.
        gatherCopy(
            output_.get(),
            outputRow,
            outputSize,
            spillSources_,
            spillSourceRows_,
            {});
        outputRow += outputSize;
        outputSize = 0;
      }
      // Advance the stream.
      stream->pop();
    }
    VELOX_CHECK_EQ(outputRow + outputSize, output_->size());

    if (FOLLY_LIKELY(outputSize != 0)) {
      gatherCopy(
          output_.get(),
          outputRow,
          outputSize,
          spillSources_,
          spillSourceRows_,
          {});
    }
  } else {
    VELOX_CHECK_NOT_NULL(batchStreamReader_);
    RowVectorPtr output;
    batchStreamReader_->nextBatch(output);
    output_ = std::move(output);
  }

  numOutputRows_ += output_->size();
}

void NonMaterializedSortBuffer::finishInputSpill() {
  VELOX_CHECK_NULL(spillMerger_);
  SpillPartitionSet spillPartitionSet;
  VELOX_CHECK_NOT_NULL(inputSpiller_);
  VELOX_CHECK_NULL(outputSpiller_);
  VELOX_CHECK(!inputSpiller_->finalized());
  inputSpiller_->finishSpill(spillPartitionSet);
  VELOX_CHECK_EQ(spillPartitionSet.size(), 1);
  inputSpillFileGroups_.push_back(spillPartitionSet.begin()->second->files());
}

void NonMaterializedSortBuffer::finishOutputSpill() {
  VELOX_CHECK_NULL(spillMerger_);
  VELOX_CHECK(outputSpillPartitionSet_.empty());
  VELOX_CHECK_NULL(inputSpiller_);
  VELOX_CHECK_NOT_NULL(outputSpiller_);
  VELOX_CHECK(!outputSpiller_->finalized());
  outputSpiller_->finishSpill(outputSpillPartitionSet_);
  VELOX_CHECK_EQ(outputSpillPartitionSet_.size(), 1);
}

void NonMaterializedSortBuffer::prepareOutputWithSpill() {
  VELOX_CHECK(hasSpilled());
  if (inputSpiller_ != nullptr) {
    if (spillMerger_ != nullptr) {
      VELOX_CHECK(inputSpillFileGroups_.empty());
      return;
    }

    std::vector<std::unique_ptr<SpillMergeStream>> spillStreams;
    int index = 0;
    for (const auto& spillFiles : inputSpillFileGroups_) {
      std::vector<std::unique_ptr<SpillReadFile>> spillReadFiles;
      spillReadFiles.reserve(spillFiles.size());
      for (const auto& spillFile : spillFiles) {
        spillReadFiles.emplace_back(
            SpillReadFile::create(
                spillFile, spillConfig_->readBufferSize, pool_, spillStats_));
      }
      auto stream = ConcatFilesSpillMergeStream::create(
          index++, std::move(spillReadFiles));
      spillStreams.push_back(std::move(stream));
    }
    inputSpillFileGroups_.clear();
    spillMerger_ = std::make_unique<TreeOfLosers<SpillMergeStream>>(
        std::move(spillStreams));
  } else {
    VELOX_CHECK_NOT_NULL(outputSpiller_);
    if (batchStreamReader_ != nullptr) {
      VELOX_CHECK(outputSpillPartitionSet_.empty());
      return;
    }
    VELOX_CHECK_EQ(outputSpillPartitionSet_.size(), 1);
    batchStreamReader_ =
        outputSpillPartitionSet_.begin()->second->createUnorderedReader(
            spillConfig_->readBufferSize, pool(), spillStats_);
  }
  outputSpillPartitionSet_.clear();
}
} // namespace facebook::velox::exec
