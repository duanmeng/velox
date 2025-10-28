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
#include <connectors/hive/HiveConnector.h>
#include <connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h>
#include <dwio/parquet/RegisterParquetReader.h>
#include <exec/tests/utils/HiveConnectorTestBase.h>
#include <folly/init/Init.h>
#include <functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <functions/prestosql/registration/RegistrationFunctions.h>
#include <parse/TypeResolver.h>

#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/Expressions.h"

namespace boltrain::kafka {

namespace Velox = facebook::velox;
namespace VeloxExec = facebook::velox::exec;
namespace VeloxConnector = facebook::velox::connector;
namespace VeloxMemory = facebook::velox::memory;
namespace VeloxCommon = facebook::velox::common;

struct KafkaConnectorSplit : public VeloxConnector::ConnectorSplit {
  explicit KafkaConnectorSplit(const std::string& connectorId, size_t numRows)
      : ConnectorSplit(connectorId), numRows(numRows) {}
  // Row many rows to fetch.
  size_t numRows;
};

class KafkaTableHandle final : public VeloxConnector::ConnectorTableHandle {
 public:
  explicit KafkaTableHandle(std::string connectorId)
      : ConnectorTableHandle(std::move(connectorId)) {}

  const std::string& name() const override {
    static const std::string kName = "Kafka-mock-table";
    return kName;
  }
};

class KafkaDataSource final : public VeloxConnector::DataSource {
 public:
  KafkaDataSource(
      const Velox::RowTypePtr& outputType,
      const VeloxConnector::ConnectorTableHandlePtr& tableHandle,
      VeloxMemory::MemoryPool* pool)
      : outputType_(outputType),
        pool_(pool),
        vectorFuzzer_(
            std::make_unique<Velox::VectorFuzzer>(
                Velox::VectorFuzzer::Options{},
                pool_)) {}

  void addSplit(std::shared_ptr<VeloxConnector::ConnectorSplit> split) override;

  void addDynamicFilter(
      Velox::column_index_t /*outputChannel*/,
      const std::shared_ptr<VeloxCommon::Filter>& /*filter*/) override {
    VELOX_NYI("Dynamic filters not supported by KafkaConnector.");
  }

  std::optional<Velox::RowVectorPtr> next(
      uint64_t size,
      Velox::ContinueFuture& future) override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

 private:
  const Velox::RowTypePtr outputType_;
  VeloxMemory::MemoryPool* const pool_;
  const std::unique_ptr<Velox::VectorFuzzer> vectorFuzzer_;
  // The current split being processed.
  std::shared_ptr<KafkaConnectorSplit> split_;

  // How many rows were generated for this split.
  uint64_t splitOffset_{0};
  uint64_t splitEnd_{0};

  size_t completedRows_{0};
  size_t completedBytes_{0};
};

class KafkaConnector final : public VeloxConnector::Connector {
 public:
  KafkaConnector(
      const std::string& id,
      std::shared_ptr<const Velox::config::ConfigBase> config,
      folly::Executor* /*executor*/)
      : VeloxConnector::Connector(id) {}

  std::unique_ptr<VeloxConnector::DataSource> createDataSource(
      const Velox::RowTypePtr& outputType,
      const VeloxConnector::ConnectorTableHandlePtr& tableHandle,
      const VeloxConnector::ColumnHandleMap& /*columnHandles*/,
      VeloxConnector::ConnectorQueryCtx* connectorQueryCtx) override final {
    return std::make_unique<KafkaDataSource>(
        outputType, tableHandle, connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<VeloxConnector::DataSink> createDataSink(
      Velox::RowTypePtr /*inputType*/,
      VeloxConnector::
          ConnectorInsertTableHandlePtr /*connectorInsertTableHandle*/,
      VeloxConnector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      VeloxConnector::CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI("KafkaConnector does not support data sink.");
  }
};

} // namespace boltrain::kafka
