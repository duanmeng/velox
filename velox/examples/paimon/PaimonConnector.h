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
#include "velox/connectors/hive/HiveConnector.h"

namespace facebook::velox::connector::paimon {

class PaimonConnector : public Connector {
 public:
  PaimonConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor);

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const connector::ColumnHandleMap& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      ConnectorInsertTableHandlePtr connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override {
    VELOX_UNSUPPORTED("Paimon write not supported yet");
  }

  folly::Executor* ioExecutor() const override {
    return ioExecutor_;
  }

  static void registerSerDe();

 private:
  // Reuse HiveConfig for file system and caching configurations
  const std::shared_ptr<hive::HiveConfig> hiveConfig_;
  FileHandleFactory fileHandleFactory_;
  folly::Executor* ioExecutor_;
};

class PaimonConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kPaimonConnectorName = "paimon";

  PaimonConnectorFactory() : ConnectorFactory(kPaimonConnectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      [[maybe_unused]] folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<PaimonConnector>(id, config, ioExecutor);
  }
};

} // namespace facebook::velox::connector::paimon
