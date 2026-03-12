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

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::paimon {

/// Represents metadata for a single physical file within a Paimon Split
struct PaimonDataFileMeta {
  std::string filePath;
  uint64_t length;
  int32_t level; // LSM level
  // Add minKey/maxKey here if needed for client-side skipping or merge logic
};

/// Represents a Paimon DataSplit (a collection of files in one bucket)
class PaimonConnectorSplit : public connector::ConnectorSplit {
 public:
  PaimonConnectorSplit(
      const std::string& connectorId,
      std::vector<PaimonDataFileMeta> files,
      std::unordered_map<std::string, std::optional<std::string>> partitionKeys,
      std::optional<int32_t> bucketId)
      : ConnectorSplit(connectorId),
        files_(std::move(files)),
        partitionKeys_(std::move(partitionKeys)),
        bucketId_(bucketId) {}

  ~PaimonConnectorSplit() override = default;

  std::string toString() const override {
    return fmt::format(
        "PaimonSplit: {} files, bucket {}",
        files_.size(),
        bucketId_.value_or(-1));
  }

  folly::dynamic serialize() const override {
    // Implementation omitted for brevity
    return folly::dynamic::object;
  }

  const std::vector<PaimonDataFileMeta>& files() const {
    return files_;
  }
  const std::unordered_map<std::string, std::optional<std::string>>&
  partitionKeys() const {
    return partitionKeys_;
  }
  std::optional<int32_t> bucketId() const {
    return bucketId_;
  }

 private:
  const std::vector<PaimonDataFileMeta> files_;
  const std::unordered_map<std::string, std::optional<std::string>>
      partitionKeys_;
  const std::optional<int32_t> bucketId_;
};

} // namespace facebook::velox::connector::paimon
