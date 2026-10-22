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
#include "velox/connectors/hive/paimon/PaimonTableHandle.h"

#include <folly/json.h>
#include <algorithm>
#include <cmath>
#include <set>
#include <sstream>
#include <unordered_set>

#include "velox/core/Expressions.h"

namespace facebook::velox::connector::hive::paimon {
namespace {

std::map<std::string, const common::SubfieldFilters::value_type*> sortedFilters(
    const common::SubfieldFilters& filters) {
  std::map<std::string, const common::SubfieldFilters::value_type*> result;
  for (const auto& entry : filters) {
    result.emplace(folly::toJson(serializeSubfield(entry.first)), &entry);
  }
  return result;
}

std::map<int64_t, PaimonSchemaPtr> makeSchemaMap(
    std::vector<PaimonSchemaPtr> schemas) {
  std::map<int64_t, PaimonSchemaPtr> result;
  for (auto& schema : schemas) {
    VELOX_USER_CHECK_NOT_NULL(schema, "Missing Paimon schema");
    const auto id = schema->schemaId();
    VELOX_USER_CHECK(
        result.emplace(id, std::move(schema)).second,
        "Duplicate Paimon schema ID: {}",
        id);
  }
  return result;
}

void checkExpressionType(const TypePtr& actual, const TypePtr& expected) {
  VELOX_USER_CHECK(
      actual && *actual == *expected,
      "Paimon expression field type does not match {}",
      expected->toString());
}

// Validate references without compiling or evaluating the expression. Each
// lambda gets its own visited set because a shared expression can bind to
// different fields under different lexical scopes.
void collectFilterColumns(
    const core::TypedExprPtr& expr,
    const RowTypePtr& rowType,
    std::vector<RowTypePtr>& scopes,
    std::unordered_set<const core::ITypedExpr*>& visited,
    std::set<column_index_t>& columns) {
  VELOX_USER_CHECK_NOT_NULL(expr, "Missing Paimon filter expression input");
  if (!visited.insert(expr.get()).second) {
    return;
  }
  if (auto lambda = core::TypedExprs::asLambda(expr)) {
    std::unordered_set<std::string> names;
    for (const auto& name : lambda->signature()->names()) {
      VELOX_USER_CHECK(
          names.insert(name).second, "Duplicate lambda parameter: {}", name);
    }
    scopes.push_back(lambda->signature());
    std::unordered_set<const core::ITypedExpr*> lambdaVisited;
    collectFilterColumns(
        lambda->body(), rowType, scopes, lambdaVisited, columns);
    scopes.pop_back();
    return;
  }
  if (auto field = core::TypedExprs::asFieldAccess(expr)) {
    if (field->isInputColumn()) {
      for (auto it = scopes.rbegin(); it != scopes.rend(); ++it) {
        if (auto index = (*it)->getChildIdxIfExists(field->name())) {
          checkExpressionType(field->type(), (*it)->childAt(*index));
          return;
        }
      }
      const auto index = rowType->getChildIdxIfExists(field->name());
      VELOX_USER_CHECK(
          index.has_value(), "Unknown Paimon filter field: {}", field->name());
      checkExpressionType(field->type(), rowType->childAt(*index));
      columns.insert(*index);
      return;
    }
    const auto& inputType = field->inputs().front()->type();
    VELOX_USER_CHECK(
        inputType && inputType->isRow(),
        "Paimon field access requires ROW input");
    const auto index = inputType->asRow().getChildIdxIfExists(field->name());
    VELOX_USER_CHECK(
        index.has_value(), "Unknown Paimon filter subfield: {}", field->name());
    checkExpressionType(field->type(), inputType->childAt(*index));
  } else if (
      auto dereference =
          std::dynamic_pointer_cast<const core::DereferenceTypedExpr>(expr)) {
    const auto& inputType = dereference->inputs().front()->type();
    VELOX_USER_CHECK(
        inputType && inputType->isRow(),
        "Paimon dereference requires ROW input");
    VELOX_USER_CHECK_LT(
        dereference->index(),
        inputType->size(),
        "Invalid Paimon filter field index");
    checkExpressionType(
        dereference->type(), inputType->childAt(dereference->index()));
  } else if (expr->isInputKind()) {
    checkExpressionType(expr->type(), rowType);
    for (column_index_t i = 0; i < rowType->size(); ++i) {
      columns.insert(i);
    }
  }
  for (const auto& input : expr->inputs()) {
    collectFilterColumns(input, rowType, scopes, visited, columns);
  }
}

} // namespace

PaimonTableHandle::PaimonTableHandle(
    std::string connectorId,
    std::string tableName,
    std::string tablePath,
    PaimonTableType tableType,
    int64_t targetSchemaId,
    std::vector<PaimonSchemaPtr> schemas,
    common::SubfieldFilters subfieldFilters,
    core::TypedExprPtr remainingFilter,
    ReadMode readMode,
    std::unordered_map<std::string, std::string> readOptions,
    std::vector<PaimonColumnHandlePtr> filterColumnHandles,
    double sampleRate,
    std::string dbName)
    : FileTableHandle(std::move(connectorId)),
      tableName_(std::move(tableName)),
      tablePath_(std::move(tablePath)),
      tableType_(tableType),
      targetSchemaId_(targetSchemaId),
      schemas_(makeSchemaMap(std::move(schemas))),
      subfieldFilters_(std::move(subfieldFilters)),
      remainingFilter_(std::move(remainingFilter)),
      readMode_(readMode),
      readOptions_(std::move(readOptions)),
      sampleRate_(sampleRate),
      dbName_(std::move(dbName)),
      filterColumnHandles_(std::move(filterColumnHandles)) {
  VELOX_USER_CHECK(
      !this->connectorId().empty(), "Paimon connector ID must not be empty");
  VELOX_USER_CHECK(!tableName_.empty(), "Paimon table name must not be empty");
  VELOX_USER_CHECK(!tablePath_.empty(), "Paimon table path must not be empty");
  VELOX_USER_CHECK_GE(
      targetSchemaId_, 0, "Paimon target schema ID must be non-negative");
  VELOX_USER_CHECK(
      tableType_ == PaimonTableType::kAppendOnly ||
          tableType_ == PaimonTableType::kPrimaryKey,
      "Invalid Paimon table type: {}",
      static_cast<int>(tableType_));
  readModeName(readMode_);
  VELOX_USER_CHECK(
      std::isfinite(sampleRate_) && sampleRate_ > 0 && sampleRate_ <= 1,
      "Paimon sample rate must be in (0, 1]");

  const auto& target = targetSchema();
  for (const auto& [_, schema] : schemas_) {
    VELOX_USER_CHECK_EQ(
        schema->primaryKeys().empty(),
        tableType_ == PaimonTableType::kAppendOnly,
        "Paimon table type disagrees with primary keys in schema {}",
        schema->schemaId());
  }
  tableParameters_ = target->options();
  for (const auto& [key, value] : readOptions_) {
    VELOX_USER_CHECK(!key.empty(), "Paimon read option name must not be empty");
    tableParameters_.insert_or_assign(key, value);
  }

  const auto& rowType = target->rowType();
  const auto& fields = target->metadata().children;
  const std::unordered_set<int32_t> partitions(
      partitionKeys().begin(), partitionKeys().end());
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (column_index_t i = 0; i < rowType->size(); ++i) {
    if (partitions.count(*fields[i].fieldId) == 0) {
      names.push_back(rowType->nameOf(i));
      types.push_back(rowType->childAt(i));
    }
  }
  dataColumns_ = ROW(std::move(names), std::move(types));

  std::set<column_index_t> filterColumns;
  for (const auto& [subfield, filter] : subfieldFilters_) {
    VELOX_USER_CHECK_NOT_NULL(filter, "Missing Paimon subfield filter");
    const auto path = target->fieldIdPath(subfield);
    filterColumns.insert(target->fieldIndexPath(path.front()).front());
  }
  if (remainingFilter_) {
    VELOX_USER_CHECK(
        remainingFilter_->type() && remainingFilter_->type()->isBoolean(),
        "Paimon remaining filter must return BOOLEAN");
    std::vector<RowTypePtr> scopes;
    std::unordered_set<const core::ITypedExpr*> visited;
    collectFilterColumns(
        remainingFilter_, rowType, scopes, visited, filterColumns);
  }
  std::unordered_set<int32_t> filterIds;
  for (const auto& column : filterColumnHandles_) {
    VELOX_USER_CHECK_NOT_NULL(column, "Missing Paimon filter column handle");
    validateColumn(*column);
    VELOX_USER_CHECK(
        filterIds.insert(column->fieldId()).second,
        "Duplicate Paimon filter column: {}",
        column->name());
  }
  for (auto index : filterColumns) {
    const auto id = *fields[index].fieldId;
    if (filterIds.insert(id).second) {
      filterColumnHandles_.push_back(columnHandle(id));
    }
  }
}

const PaimonSchemaPtr& PaimonTableHandle::schema(int64_t schemaId) const {
  const auto it = schemas_.find(schemaId);
  VELOX_USER_CHECK(
      it != schemas_.end(),
      "Missing Paimon schema {} for table {}",
      schemaId,
      tableName_);
  return it->second;
}

PaimonColumnHandlePtr PaimonTableHandle::columnHandle(
    int32_t fieldId,
    std::vector<common::Subfield> requiredSubfields) const {
  const auto& target = targetSchema();
  const auto& path = target->fieldIndexPath(fieldId);
  VELOX_USER_CHECK_EQ(
      path.size(), 1, "Paimon column handles require a top-level field ID");
  const auto index = path.front();
  const auto isPartition =
      std::find(partitionKeys().begin(), partitionKeys().end(), fieldId) !=
      partitionKeys().end();
  return std::make_shared<PaimonColumnHandle>(
      target->rowType()->nameOf(index),
      isPartition ? FileColumnHandle::ColumnType::kPartitionKey
                  : FileColumnHandle::ColumnType::kRegular,
      target->rowType()->childAt(index),
      targetSchemaId_,
      target->metadata().children[index],
      std::move(requiredSubfields));
}

void PaimonTableHandle::validateColumn(const PaimonColumnHandle& column) const {
  VELOX_USER_CHECK_EQ(
      column.schemaId(),
      targetSchemaId_,
      "Paimon column must use the target schema");
  const auto& target = targetSchema();
  const auto& path = target->fieldIndexPath(column.fieldId());
  VELOX_USER_CHECK_EQ(
      path.size(), 1, "Paimon column handles require a top-level field ID");
  const auto index = path.front();
  VELOX_USER_CHECK_EQ(
      column.name(),
      target->rowType()->nameOf(index),
      "Paimon column name does not match its field ID");
  VELOX_USER_CHECK(
      *column.dataType() == *target->rowType()->childAt(index),
      "Paimon column type does not match the target schema");
  VELOX_USER_CHECK(
      column.metadata() == target->metadata().children[index],
      "Paimon column metadata does not match the target schema");
  const auto isPartition =
      std::find(
          partitionKeys().begin(), partitionKeys().end(), column.fieldId()) !=
      partitionKeys().end();
  VELOX_USER_CHECK_EQ(
      column.isPartitionKey(),
      isPartition,
      "Paimon column role does not match partition keys");
}

void PaimonTableHandle::validateAssignments(
    const RowTypePtr& outputType,
    const ColumnHandleMap& assignments) const {
  VELOX_USER_CHECK_NOT_NULL(outputType, "Missing Paimon output type");
  std::unordered_set<int32_t> ids;
  for (const auto& [alias, handle] : assignments) {
    VELOX_USER_CHECK(!alias.empty(), "Paimon output alias must not be empty");
    const auto* column = dynamic_cast<const PaimonColumnHandle*>(handle.get());
    VELOX_USER_CHECK_NOT_NULL(
        column, "Paimon assignments require PaimonColumnHandle");
    validateColumn(*column);
    VELOX_USER_CHECK(
        ids.insert(column->fieldId()).second || column->isPartitionKey(),
        "Duplicate Paimon regular column assignments require a Project: {}",
        column->name());
  }
  std::unordered_set<std::string> names;
  for (column_index_t i = 0; i < outputType->size(); ++i) {
    const auto& name = outputType->nameOf(i);
    VELOX_USER_CHECK(
        names.insert(name).second, "Duplicate Paimon output column: {}", name);
    const auto it = assignments.find(name);
    VELOX_USER_CHECK(
        it != assignments.end(), "Missing Paimon assignment: {}", name);
    const auto& column = static_cast<const PaimonColumnHandle&>(*it->second);
    VELOX_USER_CHECK(
        *column.dataType() == *outputType->childAt(i),
        "Paimon output type does not match assignment: {}",
        name);
  }
}

std::string PaimonTableHandle::toString() const {
  std::ostringstream out;
  out << "PaimonTableHandle [name: " << tableName_ << ", path: " << tablePath_
      << ", tableType: " << tableType_
      << ", targetSchemaId: " << targetSchemaId_
      << ", readMode: " << readModeName(readMode_)
      << ", sampleRate: " << sampleRate_;
  for (const auto& [_, entry] : sortedFilters(subfieldFilters_)) {
    out << ", filter: (" << entry->first.toString() << ", "
        << entry->second->toString() << ")";
  }
  if (remainingFilter_) {
    out << ", remainingFilter: " << remainingFilter_->toString();
  }
  out << "]";
  return out.str();
}

folly::dynamic PaimonTableHandle::serialize() const {
  auto obj = ConnectorTableHandle::serializeBase("PaimonTableHandle");
  obj["version"] = kSerdeVersion;
  obj["tableName"] = tableName_;
  obj["tablePath"] = tablePath_;
  obj["tableType"] = paimonTableTypeString(tableType_);
  obj["targetSchemaId"] = targetSchemaId_;
  obj["schemas"] = folly::dynamic::array;
  for (const auto& [_, schema] : schemas_) {
    obj["schemas"].push_back(schema->serialize());
  }
  obj["subfieldFilters"] = folly::dynamic::array;
  for (const auto& [_, entry] : sortedFilters(subfieldFilters_)) {
    obj["subfieldFilters"].push_back(
        folly::dynamic::object("subfield", serializeSubfield(entry->first))(
            "filter", entry->second->serialize()));
  }
  obj["remainingFilter"] = remainingFilter_ ? remainingFilter_->serialize()
                                            : folly::dynamic(nullptr);
  obj["readMode"] = readModeName(readMode_);
  obj["readOptions"] = folly::dynamic::object;
  for (const auto& [key, value] : readOptions_) {
    obj["readOptions"][key] = value;
  }
  obj["filterColumnHandles"] = folly::dynamic::array;
  for (const auto& column : filterColumnHandles_) {
    obj["filterColumnHandles"].push_back(column->serialize());
  }
  obj["sampleRate"] = sampleRate_;
  obj["dbName"] = dbName_;
  return obj;
}

ConnectorTableHandlePtr PaimonTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  VELOX_USER_CHECK(obj.isObject(), "Expected PaimonTableHandle object");
  for (const auto* key :
       {"connectorId",
        "tableName",
        "tablePath",
        "tableType",
        "readMode",
        "dbName"}) {
    VELOX_USER_CHECK(obj.at(key).isString(), "Expected Paimon {} string", key);
  }
  VELOX_USER_CHECK(
      obj.at("sampleRate").isInt() || obj.at("sampleRate").isDouble(),
      "Expected Paimon sample rate number");
  VELOX_USER_CHECK(
      obj.at("version").isInt() && obj.at("version").asInt() == kSerdeVersion,
      "Unsupported PaimonTableHandle version");
  VELOX_USER_CHECK(
      obj.at("targetSchemaId").isInt(), "Expected Paimon target schema ID");
  VELOX_USER_CHECK(
      obj.at("schemas").isArray(), "Expected Paimon schemas array");
  std::vector<PaimonSchemaPtr> schemas;
  for (const auto& schema : obj.at("schemas")) {
    schemas.push_back(PaimonSchema::create(schema));
  }
  VELOX_USER_CHECK(
      obj.at("subfieldFilters").isArray(),
      "Expected Paimon subfield filters array");
  common::SubfieldFilters filters;
  for (const auto& entry : obj.at("subfieldFilters")) {
    const auto filter =
        ISerializable::deserialize<common::Filter>(entry.at("filter"));
    VELOX_USER_CHECK_NOT_NULL(filter, "Invalid Paimon subfield filter");
    VELOX_USER_CHECK(
        filters
            .emplace(deserializeSubfield(entry.at("subfield")), filter->clone())
            .second,
        "Duplicate Paimon subfield filter");
  }
  core::TypedExprPtr remainingFilter;
  if (!obj.at("remainingFilter").isNull()) {
    remainingFilter = ISerializable::deserialize<core::ITypedExpr>(
        obj.at("remainingFilter"), context);
    VELOX_USER_CHECK_NOT_NULL(
        remainingFilter, "Invalid Paimon remaining filter");
  }
  VELOX_USER_CHECK(
      obj.at("readOptions").isObject(), "Expected Paimon read options object");
  std::unordered_map<std::string, std::string> options;
  for (const auto& [key, value] : obj.at("readOptions").items()) {
    VELOX_USER_CHECK(
        key.isString() && value.isString(),
        "Paimon read options must be strings");
    options.emplace(key.asString(), value.asString());
  }
  VELOX_USER_CHECK(
      obj.at("filterColumnHandles").isArray(),
      "Expected Paimon filter columns array");
  std::vector<PaimonColumnHandlePtr> filterColumns;
  for (const auto& column : obj.at("filterColumnHandles")) {
    filterColumns.push_back(
        ISerializable::deserialize<PaimonColumnHandle>(column));
  }
  return std::make_shared<PaimonTableHandle>(
      obj.at("connectorId").asString(),
      obj.at("tableName").asString(),
      obj.at("tablePath").asString(),
      paimonTableTypeFromString(obj.at("tableType").asString()),
      obj.at("targetSchemaId").asInt(),
      std::move(schemas),
      std::move(filters),
      std::move(remainingFilter),
      readModeFromName(obj.at("readMode").asString()),
      std::move(options),
      std::move(filterColumns),
      obj.at("sampleRate").asDouble(),
      obj.at("dbName").asString());
}

void PaimonTableHandle::registerSerDe() {
  DeserializationWithContextRegistryForSharedPtr().Register(
      "PaimonTableHandle", create);
}

std::string PaimonTableHandle::readModeName(ReadMode mode) {
  switch (mode) {
    case ReadMode::kSnapshot:
      return "SNAPSHOT";
    case ReadMode::kIncremental:
      return "INCREMENTAL";
    case ReadMode::kStreaming:
      return "STREAMING";
  }
  VELOX_USER_FAIL("Unsupported Paimon read mode: {}", static_cast<int>(mode));
}

PaimonTableHandle::ReadMode PaimonTableHandle::readModeFromName(
    const std::string& name) {
  if (name == "SNAPSHOT") {
    return ReadMode::kSnapshot;
  }
  if (name == "INCREMENTAL") {
    return ReadMode::kIncremental;
  }
  if (name == "STREAMING") {
    return ReadMode::kStreaming;
  }
  VELOX_USER_FAIL("Unsupported Paimon read mode: {}", name);
}

} // namespace facebook::velox::connector::hive::paimon
