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
#include "velox/examples/paimon/PaimonConnector.h"
#include "velox/examples/paimon/PaimonConnectorSplit.h"
#include "velox/examples/paimon/PaimonDataSource.h"

namespace facebook::velox::connector::paimon {

PaimonConnector::PaimonConnector(
    const std::string& id,
    std::shared_ptr<const config::ConfigBase> config,
    folly::Executor* ioExecutor)
    : Connector(id, std::move(config)),
      hiveConfig_(std::make_shared<hive::HiveConfig>(connectorConfig())),
      fileHandleFactory_(
          hiveConfig_->isFileHandleCacheEnabled()
              ? std::make_unique<SimpleLRUCache<FileHandleKey, FileHandle>>(
                    hiveConfig_->numCacheFileHandles())
              : nullptr,
          std::make_unique<FileHandleGenerator>(hiveConfig_->config())),
      ioExecutor_(ioExecutor) {}

std::unique_ptr<DataSource> PaimonConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<PaimonDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      connectorQueryCtx,
      ioExecutor_,
      &fileHandleFactory_,
      hiveConfig_);
}

void PaimonConnector::registerSerDe() {
  // Register PaimonConnectorSplit deserialization logic here
  // (Implementation similar to HiveConnectorSplit::registerSerDe)
}

} // namespace facebook::velox::connector::paimon
