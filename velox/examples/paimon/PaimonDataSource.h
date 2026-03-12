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

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/SplitReader.h"
#include "velox/examples/paimon/PaimonConnectorSplit.h"

namespace facebook::velox::connector::paimon {

class PaimonDataSource : public connector::DataSource {
 public:
  PaimonDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<const connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<const connector::ColumnHandle>>& columnHandles,
      connector::ConnectorQueryCtx* connectorQueryCtx,
      folly::Executor* ioExecutor,
      facebook::velox::FileHandleFactory* fileHandleFactory,
      std::shared_ptr<const hive::HiveConfig> hiveConfig);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) override {
    // Pass to ScanSpec if supported in future
  }

  uint64_t getCompletedBytes() override {
    return 0;
  }

  uint64_t getCompletedRows() override {
    return 0;
  }

  std::unordered_map<std::string, RuntimeMetric> getRuntimeStats() override {
    return {};
  }

  int64_t estimatedRowSize() override {
    return kUnknownRowSize;
  }

 private:
  void createSplitReader();

  const RowTypePtr outputType_;
  std::shared_ptr<hive::HiveTableHandle> hiveTableHandle_;
  connector::ConnectorQueryCtx* connectorQueryCtx_;
  folly::Executor* ioExecutor_;
  facebook::velox::FileHandleFactory* fileHandleFactory_;
  std::shared_ptr<const hive::HiveConfig> hiveConfig_;

  // Current split information
  std::shared_ptr<PaimonConnectorSplit> split_;
  size_t currentFileIndex_{0};

  // The underlying reader
  std::unique_ptr<hive::SplitReader> splitReader_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  std::shared_ptr<io::IoStatistics> ioStatistics_;

  RowVectorPtr output_;
};

} // namespace facebook::velox::connector::paimon
