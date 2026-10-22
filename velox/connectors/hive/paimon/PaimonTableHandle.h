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

#include <map>

#include "velox/connectors/hive/FileTableHandle.h"
#include "velox/connectors/hive/paimon/PaimonColumnHandle.h"
#include "velox/connectors/hive/paimon/PaimonConnectorSplit.h"

namespace facebook::velox::connector::hive::paimon {

/// Query identity and logical scan requirements for a Paimon table. The caller
/// supplies a fixed target and all required historical schemas. This handle
/// neither resolves snapshots nor maps historical fields to physical columns.
/// Encoding a read mode or option does not enable its execution.
class PaimonTableHandle final : public FileTableHandle {
 public:
  static constexpr int32_t kSerdeVersion = 1;

  enum class ReadMode { kSnapshot, kIncremental, kStreaming };

  PaimonTableHandle(
      std::string connectorId,
      std::string tableName,
      std::string tablePath,
      PaimonTableType tableType,
      int64_t targetSchemaId,
      std::vector<PaimonSchemaPtr> schemas,
      common::SubfieldFilters subfieldFilters = {},
      core::TypedExprPtr remainingFilter = nullptr,
      ReadMode readMode = ReadMode::kSnapshot,
      std::unordered_map<std::string, std::string> readOptions = {},
      std::vector<PaimonColumnHandlePtr> filterColumnHandles = {},
      double sampleRate = 1.0,
      std::string dbName = "");

  const std::string& name() const override {
    return tableName_;
  }

  const std::string& tablePath() const {
    return tablePath_;
  }

  PaimonTableType tableType() const {
    return tableType_;
  }

  int64_t targetSchemaId() const {
    return targetSchemaId_;
  }

  const std::map<int64_t, PaimonSchemaPtr>& schemas() const {
    return schemas_;
  }

  /// Missing history is an error, never a request to use the target schema.
  const PaimonSchemaPtr& schema(int64_t schemaId) const;

  const PaimonSchemaPtr& targetSchema() const {
    return schema(targetSchemaId_);
  }

  const std::vector<int32_t>& partitionKeys() const {
    return targetSchema()->partitionKeys();
  }

  const std::vector<int32_t>& primaryKeys() const {
    return targetSchema()->primaryKeys();
  }

  const common::SubfieldFilters& subfieldFilters() const override {
    return subfieldFilters_;
  }

  const core::TypedExprPtr& remainingFilter() const override {
    return remainingFilter_;
  }

  ReadMode readMode() const {
    return readMode_;
  }

  /// Query overrides, separate from each schema's historical options.
  const std::unordered_map<std::string, std::string>& readOptions() const {
    return readOptions_;
  }

  double sampleRate() const override {
    return sampleRate_;
  }

  /// Non-partition columns of the target schema, not a historical file layout.
  const RowTypePtr& dataColumns() const override {
    return dataColumns_;
  }

  /// Target schema options overlaid with query overrides. Historical schemas
  /// do not contribute defaults here. A file's format must be supplied by its
  /// own metadata, not inferred from these table options.
  const std::unordered_map<std::string, std::string>& tableParameters()
      const override {
    return tableParameters_;
  }

  /// Includes schema-derived handles for predicate columns, preserving the
  /// role of unprojected partition columns in FileDataSource preparation.
  std::vector<FileColumnHandlePtr> filterColumnHandles() const override {
    return {filterColumnHandles_.begin(), filterColumnHandles_.end()};
  }

  const std::string& dbName() const override {
    return dbName_;
  }

  /// Constructs a top-level column with its target-schema identity and role.
  PaimonColumnHandlePtr columnHandle(
      int32_t fieldId,
      std::vector<common::Subfield> requiredSubfields = {}) const;

  void validateColumn(const PaimonColumnHandle& column) const;

  /// Assignment keys are output aliases. Column identities remain schema
  /// names and IDs; type coercion and duplicate regular aliases need a Project.
  void validateAssignments(
      const RowTypePtr& outputType,
      const ColumnHandleMap& assignments) const;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

  static std::string readModeName(ReadMode mode);

  static ReadMode readModeFromName(const std::string& name);

 private:
  const std::string tableName_;
  const std::string tablePath_;
  const PaimonTableType tableType_;
  const int64_t targetSchemaId_;
  const std::map<int64_t, PaimonSchemaPtr> schemas_;
  const common::SubfieldFilters subfieldFilters_;
  const core::TypedExprPtr remainingFilter_;
  const ReadMode readMode_;
  const std::unordered_map<std::string, std::string> readOptions_;
  const double sampleRate_;
  const std::string dbName_;

  RowTypePtr dataColumns_;
  std::unordered_map<std::string, std::string> tableParameters_;
  std::vector<PaimonColumnHandlePtr> filterColumnHandles_;
};

using PaimonTableHandlePtr = std::shared_ptr<const PaimonTableHandle>;

} // namespace facebook::velox::connector::hive::paimon
