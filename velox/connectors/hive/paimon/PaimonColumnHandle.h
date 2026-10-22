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

#include "velox/connectors/hive/FileColumnHandle.h"
#include "velox/connectors/hive/paimon/PaimonSchema.h"

namespace facebook::velox::connector::hive::paimon {

/// A user column in a fixed target schema. 'name' is the schema name, not an
/// output alias. 'dataType' is the full target type; requiredSubfields limits
/// materialization without changing the type or identifying a physical column.
/// Only regular and partition columns are supported, without extraction or
/// coercion. Paimon system fields are not Hive row-index or row-ID columns.
class PaimonColumnHandle final : public FileColumnHandle {
 public:
  static constexpr int32_t kSerdeVersion = 1;

  PaimonColumnHandle(
      std::string name,
      ColumnType columnType,
      TypePtr dataType,
      int64_t schemaId,
      PaimonTypeMetadata metadata,
      std::vector<common::Subfield> requiredSubfields = {});

  const std::string& name() const override {
    return name_;
  }

  ColumnType columnType() const override {
    return columnType_;
  }

  const TypePtr& schemaType() const override {
    return dataType_;
  }

  int64_t schemaId() const {
    return schemaId_;
  }

  int32_t fieldId() const {
    return *metadata_.fieldId;
  }

  const PaimonTypeMetadata& metadata() const {
    return metadata_;
  }

  const std::vector<common::Subfield>& requiredSubfields() const override {
    return requiredSubfields_;
  }

  std::vector<int32_t> fieldIdPath(const common::Subfield& subfield) const {
    return metadata_.fieldIdPath(name_, dataType_, subfield);
  }

  /// Paimon partitions require typed binding, not Hive string parsing.
  bool isPartitionDateValueDaysSinceEpoch() const override;

  const std::function<void(VectorPtr&)>& postProcessor() const override {
    static const std::function<void(VectorPtr&)> kEmpty;
    return kEmpty;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ColumnHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

 private:
  const std::string name_;
  const ColumnType columnType_;
  const TypePtr dataType_;
  const int64_t schemaId_;
  const PaimonTypeMetadata metadata_;
  const std::vector<common::Subfield> requiredSubfields_;
};

using PaimonColumnHandlePtr = std::shared_ptr<const PaimonColumnHandle>;

} // namespace facebook::velox::connector::hive::paimon
