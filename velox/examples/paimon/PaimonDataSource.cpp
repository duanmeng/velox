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
#include "velox/examples/paimon/PaimonDataSource.h"
#include <iostream>
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/ScanSpec.h"

namespace facebook::velox::connector::paimon {

PaimonDataSource::PaimonDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<const connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<const connector::ColumnHandle>>& columnHandles,
    connector::ConnectorQueryCtx* connectorQueryCtx,
    folly::Executor* ioExecutor,
    facebook::velox::FileHandleFactory* fileHandleFactory,
    std::shared_ptr<const hive::HiveConfig> hiveConfig)
    : outputType_(outputType),
      connectorQueryCtx_(connectorQueryCtx),
      ioExecutor_(ioExecutor),
      fileHandleFactory_(fileHandleFactory),
      hiveConfig_(std::move(hiveConfig)) {
  hiveTableHandle_ = std::dynamic_pointer_cast<hive::HiveTableHandle>(
      std::const_pointer_cast<connector::ConnectorTableHandle>(tableHandle));
  VELOX_CHECK_NOT_NULL(
      hiveTableHandle_, "Table handle must be HiveTableHandle");

  ioStatistics_ = std::make_shared<io::IoStatistics>();
}

void PaimonDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NOT_NULL(split, "Split cannot be null");
  split_ = std::dynamic_pointer_cast<PaimonConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Split must be PaimonConnectorSplit");

  currentFileIndex_ = 0;

  // Initialize ScanSpec once for the query
  scanSpec_ = std::make_shared<common::ScanSpec>("root");
  scanSpec_->addAllChildFields(*outputType_);

  createSplitReader();
}

void PaimonDataSource::createSplitReader() {
  if (currentFileIndex_ >= split_->files().size()) {
    splitReader_.reset();
    return;
  }

  const auto& fileMeta = split_->files()[currentFileIndex_];

  auto format = dwio::common::FileFormat::UNKNOWN;
  if (fileMeta.filePath.length() > 8 &&
      fileMeta.filePath.substr(fileMeta.filePath.length() - 8) == ".parquet") {
    format = dwio::common::FileFormat::PARQUET;
  } else {
    format = dwio::common::FileFormat::DWRF;
  }

  auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
      split_->connectorId, // connectorId
      fileMeta.filePath, // filePath
      format, // fileFormat
      0, // start
      fileMeta.length, // length
      split_->partitionKeys(),
      split_->bucketId());

  splitReader_ = hive::SplitReader::create(
      hiveSplit,
      hiveTableHandle_,
      nullptr, // partitionKeys map (already in split)
      connectorQueryCtx_,
      hiveConfig_,
      outputType_,
      ioStatistics_,
      nullptr, // ioStats
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_);

  splitReader_->configureReaderOptions(nullptr);

  dwio::common::RuntimeStatistics runtimeStats;
  splitReader_->prepareSplit(nullptr, runtimeStats);
}

std::optional<RowVectorPtr> PaimonDataSource::next(
    uint64_t size,
    ContinueFuture& future) {
  while (true) {
    if (!splitReader_) {
      return RowVectorPtr(nullptr);
    }

    if (!output_) {
      output_ = BaseVector::create<RowVector>(
          outputType_, size, connectorQueryCtx_->memoryPool());
    } else {
      output_->resize(size);
    }

    VectorPtr baseOutput = output_;
    uint64_t rowsRead = splitReader_->next(size, baseOutput);

    // Update the typed pointer in case the split reader changed the vector
    if (baseOutput != output_) {
      output_ = std::dynamic_pointer_cast<RowVector>(baseOutput);
      VELOX_CHECK_NOT_NULL(
          output_, "Split reader returned non-RowVector output");
    }

    if (rowsRead > 0) {
      // Ensure all children are loaded to avoid "Resize on a lazy vector" error
      // and to ensure data validity after SplitReader might be reset.
      std::vector<VectorPtr> loadedChildren;
      loadedChildren.reserve(output_->childrenSize());
      for (const auto& child : output_->children()) {
        loadedChildren.push_back(BaseVector::loadedVectorShared(child));
      }

      // Create a temporary RowVector with loaded children but original size
      auto loadedOutput = std::make_shared<RowVector>(
          output_->pool(),
          output_->type(),
          output_->nulls(),
          output_->size(),
          std::move(loadedChildren));

      if (rowsRead < size) {
        output_ = std::dynamic_pointer_cast<RowVector>(
            loadedOutput->slice(0, rowsRead));
      } else {
        output_ = loadedOutput;
      }
      return output_;
    }

    // Current file exhausted, try next file
    currentFileIndex_++;
    createSplitReader();
  }
}

} // namespace facebook::velox::connector::paimon
