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

#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/SplitReader.h"
#include "velox/examples/paimon/PaimonConnectorSplit.h"

namespace facebook::velox::connector::paimon {

/// Abstract base class for Paimon reading strategies
class PaimonSplitReader {
 public:
  virtual ~PaimonSplitReader() = default;

  /// Reads the next batch of data. Returns number of rows read.
  virtual uint64_t next(uint64_t size, VectorPtr& output) = 0;

  /// Returns estimated row size for memory management
  virtual int64_t estimatedRowSize() const = 0;
};

/// Implementation for Append-Only tables (Concat Read)
/// Sequentially reads files one by one using hive::SplitReader
class PaimonAppendSplitReader : public PaimonSplitReader {
 public:
  PaimonAppendSplitReader(
      std::shared_ptr<PaimonConnectorSplit> paimonSplit,
      const std::shared_ptr<const hive::HiveTableHandle>& hiveTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const hive::HiveConfig>& hiveConfig,
      const RowTypePtr& readerOutputType,
      const std::shared_ptr<io::IoStatistics>& ioStatistics,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
      facebook::velox::FileHandleFactory* fileHandleFactory,
      folly::Executor* ioExecutor)
      : paimonSplit_(std::move(paimonSplit)),
        hiveTableHandle_(hiveTableHandle),
        connectorQueryCtx_(connectorQueryCtx),
        hiveConfig_(hiveConfig),
        readerOutputType_(readerOutputType),
        ioStatistics_(ioStatistics),
        scanSpec_(scanSpec),
        fileHandleFactory_(fileHandleFactory),
        ioExecutor_(ioExecutor) {}

  uint64_t next(uint64_t size, VectorPtr& output) override {
    while (true) {
      // 1. Ensure we have an active reader for the current file
      if (!currentHiveReader_) {
        if (currentFileIndex_ >= paimonSplit_->files().size()) {
          return 0; // End of split (all files read)
        }
        createNextInnerReader();
      }

      // 2. Delegate reading to the underlying Hive/ORC/Parquet reader
      uint64_t rowsRead = currentHiveReader_->next(size, output);

      // 3. If current file is exhausted, move to the next one
      if (rowsRead == 0) {
        currentHiveReader_.reset();
        currentFileIndex_++;
        continue; // Loop back to create the next reader
      }

      return rowsRead;
    }
  }

  int64_t estimatedRowSize() const override {
    if (currentHiveReader_) {
      return currentHiveReader_->estimatedRowSize();
    }
    return connector::DataSource::kUnknownRowSize;
  }

 private:
  void createNextInnerReader() {
    const auto& fileMeta = paimonSplit_->files()[currentFileIndex_];

    auto format = dwio::common::FileFormat::UNKNOWN;
    if (fileMeta.filePath.length() > 8 &&
        fileMeta.filePath.substr(fileMeta.filePath.length() - 8) ==
            ".parquet") {
      format = dwio::common::FileFormat::PARQUET;
    } else {
      format = dwio::common::FileFormat::DWRF;
    }

    // --- KEY LOGIC: Dynamic HiveConnectorSplit Construction ---
    // We wrap the single Paimon file meta into a HiveConnectorSplit to reuse
    // Velox's logic. This allows us to leverage existing capabilities like
    // partition key handling.
    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        paimonSplit_->connectorId, // connectorId
        fileMeta.filePath, // filePath
        format, // fileFormat
        0, // start
        fileMeta.length, // length
        paimonSplit_->partitionKeys(), // partitionKeys
        paimonSplit_->bucketId() // tableBucketNumber
    );

    // --- KEY LOGIC: Composition ---
    // Create a standard hive::SplitReader to handle the heavy lifting (parsing,
    // filtering, schema evolution)
    currentHiveReader_ = hive::SplitReader::create(
        hiveSplit,
        hiveTableHandle_,
        nullptr, // partitionKeys map (already in split)
        connectorQueryCtx_,
        hiveConfig_,
        readerOutputType_,
        ioStatistics_,
        nullptr, // ioStats
        fileHandleFactory_,
        ioExecutor_,
        scanSpec_);

    // Configure the reader options (e.g. file format, scan spec)
    currentHiveReader_->configureReaderOptions(nullptr);

    // Initialize the reader (prepareSplit handles metadata reading and filter
    // validation)
    dwio::common::RuntimeStatistics runtimeStats;
    currentHiveReader_->prepareSplit(nullptr, runtimeStats);
  }

  // Context required to create inner readers
  std::shared_ptr<PaimonConnectorSplit> paimonSplit_;
  std::shared_ptr<const hive::HiveTableHandle> hiveTableHandle_;
  const ConnectorQueryCtx* connectorQueryCtx_;
  std::shared_ptr<const hive::HiveConfig> hiveConfig_;
  RowTypePtr readerOutputType_;
  std::shared_ptr<io::IoStatistics> ioStatistics_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  facebook::velox::FileHandleFactory* fileHandleFactory_;
  folly::Executor* ioExecutor_;

  // Runtime state
  size_t currentFileIndex_ = 0;
  std::unique_ptr<hive::SplitReader> currentHiveReader_;
};

} // namespace facebook::velox::connector::paimon
