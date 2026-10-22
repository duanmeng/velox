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
#include "velox/connectors/hive/paimon/PaimonSchema.h"

#include <limits>
#include <tuple>
#include <unordered_set>

namespace facebook::velox::connector::hive::paimon {
namespace {

constexpr int32_t kSystemFieldIdStart = std::numeric_limits<int32_t>::max() / 2;
constexpr uint32_t kMaxTypeDepth = 128;

int32_t readInt32(const folly::dynamic& value) {
  VELOX_USER_CHECK(value.isInt(), "Expected a Paimon integer");
  const auto number = value.asInt();
  VELOX_USER_CHECK(
      number >= std::numeric_limits<int32_t>::min() &&
          number <= std::numeric_limits<int32_t>::max(),
      "Paimon integer out of range: {}",
      number);
  return static_cast<int32_t>(number);
}

void validateType(
    const TypePtr& type,
    const PaimonTypeMetadata& metadata,
    bool isField,
    std::unordered_set<int32_t>& ids,
    uint32_t depth) {
  VELOX_USER_CHECK_NOT_NULL(type, "Missing Paimon field type");
  VELOX_USER_CHECK_LE(depth, kMaxTypeDepth, "Paimon type nesting is too deep");
  VELOX_USER_CHECK_EQ(
      metadata.fieldId.has_value(),
      isField,
      "Paimon DataField IDs are required only for ROW children");
  if (isField) {
    const auto id = *metadata.fieldId;
    VELOX_USER_CHECK(
        id >= 0 && id < kSystemFieldIdStart,
        "Invalid Paimon DataField ID: {}",
        id);
    VELOX_USER_CHECK(
        ids.insert(id).second, "Duplicate Paimon field ID: {}", id);
  }

  const auto& root = metadata.logicalType;
  TypePtr expected;
  size_t parameterCount = 0;
  if (root == "BOOLEAN") {
    expected = BOOLEAN();
  } else if (root == "TINYINT") {
    expected = TINYINT();
  } else if (root == "SMALLINT") {
    expected = SMALLINT();
  } else if (root == "INTEGER") {
    expected = INTEGER();
  } else if (root == "BIGINT") {
    expected = BIGINT();
  } else if (root == "FLOAT") {
    expected = REAL();
  } else if (root == "DOUBLE") {
    expected = DOUBLE();
  } else if (root == "DATE") {
    expected = DATE();
  } else if (root == "CHAR" || root == "VARCHAR") {
    expected = VARCHAR();
    parameterCount = 1;
  } else if (root == "BINARY" || root == "VARBINARY") {
    expected = VARBINARY();
    parameterCount = 1;
  } else if (
      root == "TIMESTAMP_WITHOUT_TIME_ZONE" ||
      root == "TIMESTAMP_WITH_LOCAL_TIME_ZONE") {
    expected = TIMESTAMP();
    parameterCount = 1;
  } else if (root == "DECIMAL") {
    VELOX_USER_CHECK(
        type->isDecimal(), "Paimon DECIMAL requires a decimal type");
    parameterCount = 2;
  } else if (root == "ROW") {
    VELOX_USER_CHECK(type->isRow(), "Paimon ROW requires a ROW type");
    std::unordered_set<std::string> names;
    for (const auto& name : type->asRow().names()) {
      VELOX_USER_CHECK(!name.empty(), "Paimon field name must not be empty");
      VELOX_USER_CHECK(
          names.insert(name).second, "Duplicate Paimon field name: {}", name);
    }
  } else if (root == "ARRAY") {
    VELOX_USER_CHECK(type->isArray(), "Paimon ARRAY requires an ARRAY type");
  } else if (root == "MAP") {
    VELOX_USER_CHECK(type->isMap(), "Paimon MAP requires a MAP type");
  } else {
    VELOX_UNSUPPORTED("Unsupported Paimon logical type: {}", root);
  }
  if (expected) {
    VELOX_USER_CHECK(
        *type == *expected,
        "Paimon logical type {} does not match Velox type {}",
        root,
        type->toString());
  }
  VELOX_USER_CHECK_EQ(
      metadata.parameters.size(),
      parameterCount,
      "Invalid parameters for Paimon type {}",
      root);
  if (root == "DECIMAL") {
    const auto [precision, scale] = getDecimalPrecisionScale(*type);
    VELOX_USER_CHECK(
        metadata.parameters[0] == precision && metadata.parameters[1] == scale,
        "Paimon decimal precision/scale do not match {}",
        type->toString());
  } else if (parameterCount == 1) {
    const auto parameter = metadata.parameters[0];
    if (type->isTimestamp()) {
      VELOX_USER_CHECK(
          parameter >= 0 && parameter <= 9,
          "Invalid Paimon timestamp precision: {}",
          parameter);
    } else {
      VELOX_USER_CHECK_GT(parameter, 0, "Paimon type length must be positive");
    }
  }
  VELOX_USER_CHECK_EQ(
      metadata.children.size(),
      type->size(),
      "Paimon type metadata shape mismatch");
  for (column_index_t i = 0; i < type->size(); ++i) {
    validateType(
        type->childAt(i), metadata.children[i], type->isRow(), ids, depth + 1);
  }
}

PaimonTypeMetadata readMetadata(const folly::dynamic& obj, uint32_t depth) {
  VELOX_USER_CHECK_LE(depth, kMaxTypeDepth, "Paimon type nesting is too deep");
  VELOX_USER_CHECK(obj.isObject(), "Expected Paimon type metadata object");
  VELOX_USER_CHECK(
      obj.at("logicalType").isString(), "Expected Paimon logical type name");
  VELOX_USER_CHECK(
      obj.at("nullable").isBool(), "Expected Paimon nullability boolean");
  PaimonTypeMetadata result;
  result.logicalType = obj.at("logicalType").asString();
  result.nullable = obj.at("nullable").asBool();
  if (!obj.at("fieldId").isNull()) {
    result.fieldId = readInt32(obj.at("fieldId"));
  }
  VELOX_USER_CHECK(
      obj.at("parameters").isArray(), "Expected type parameters array");
  for (const auto& parameter : obj.at("parameters")) {
    result.parameters.push_back(readInt32(parameter));
  }
  VELOX_USER_CHECK(
      obj.at("children").isArray(), "Expected type metadata children array");
  for (const auto& child : obj.at("children")) {
    result.children.push_back(readMetadata(child, depth + 1));
  }
  return result;
}

void indexFields(
    const PaimonTypeMetadata& metadata,
    std::vector<column_index_t>& path,
    std::unordered_map<int32_t, std::vector<column_index_t>>& paths) {
  if (metadata.fieldId) {
    paths.emplace(*metadata.fieldId, path);
  }
  for (column_index_t i = 0; i < metadata.children.size(); ++i) {
    path.push_back(i);
    indexFields(metadata.children[i], path, paths);
    path.pop_back();
  }
}

std::vector<int32_t> readIds(const folly::dynamic& array) {
  VELOX_USER_CHECK(array.isArray(), "Expected Paimon field ID array");
  std::vector<int32_t> ids;
  for (const auto& value : array) {
    ids.push_back(readInt32(value));
  }
  return ids;
}

} // namespace

bool PaimonTypeMetadata::operator==(const PaimonTypeMetadata& other) const {
  return std::tie(logicalType, nullable, fieldId, parameters, children) ==
      std::tie(
             other.logicalType,
             other.nullable,
             other.fieldId,
             other.parameters,
             other.children);
}

void PaimonTypeMetadata::validate(const TypePtr& type, bool isField) const {
  std::unordered_set<int32_t> ids;
  validateType(type, *this, isField, ids, 0);
}

std::vector<int32_t> PaimonTypeMetadata::fieldIdPath(
    std::string_view name,
    const TypePtr& fieldType,
    const common::Subfield& subfield) const {
  const auto& path = subfield.path();
  VELOX_USER_CHECK(!path.empty(), "Empty Paimon field path");
  const auto* root = path.front()->as<common::Subfield::NestedField>();
  VELOX_USER_CHECK(
      root && root->name() == name,
      "Paimon field path {} must start with {}",
      subfield.toString(),
      name);
  VELOX_USER_CHECK(fieldId.has_value(), "Missing Paimon root field ID");
  std::vector<int32_t> ids{*fieldId};
  auto type = fieldType;
  const auto* metadata = this;
  for (size_t i = 1; i < path.size(); ++i) {
    const auto& element = path[i];
    column_index_t child = 0;
    if (const auto* field = element->as<common::Subfield::NestedField>()) {
      VELOX_USER_CHECK(
          type->isRow(), "ROW required in Paimon path {}", subfield.toString());
      const auto index = type->asRow().getChildIdxIfExists(field->name());
      VELOX_USER_CHECK(
          index.has_value(),
          "Unknown Paimon subfield: {}",
          subfield.toString());
      child = *index;
      ids.push_back(*metadata->children[child].fieldId);
    } else if (type->isArray()) {
      const auto* index = element->as<common::Subfield::LongSubscript>();
      VELOX_USER_CHECK(
          element->is(common::SubfieldKind::kAllSubscripts) ||
              (index && index->index() > 0),
          "Invalid ARRAY subscript in Paimon path {}",
          subfield.toString());
    } else if (type->isMap()) {
      const auto& keyType = type->childAt(0);
      VELOX_USER_CHECK(
          element->is(common::SubfieldKind::kAllSubscripts) ||
              (element->is(common::SubfieldKind::kStringSubscript) &&
               keyType->isVarchar()) ||
              (element->is(common::SubfieldKind::kLongSubscript) &&
               (keyType->isTinyint() || keyType->isSmallint() ||
                keyType->isInteger() || keyType->isBigint()) &&
               !keyType->isDate() && !keyType->isDecimal()),
          "Invalid MAP subscript in Paimon path {}",
          subfield.toString());
      child = 1;
    } else {
      VELOX_USER_FAIL("Invalid Paimon subfield path: {}", subfield.toString());
    }
    type = type->childAt(child);
    metadata = &metadata->children[child];
  }
  return ids;
}

folly::dynamic PaimonTypeMetadata::serialize() const {
  folly::dynamic obj =
      folly::dynamic::object("logicalType", logicalType)("nullable", nullable);
  obj["fieldId"] = fieldId ? folly::dynamic(*fieldId) : folly::dynamic(nullptr);
  obj["parameters"] = folly::dynamic::array;
  for (auto parameter : parameters) {
    obj["parameters"].push_back(parameter);
  }
  obj["children"] = folly::dynamic::array;
  for (const auto& child : children) {
    obj["children"].push_back(child.serialize());
  }
  return obj;
}

PaimonTypeMetadata PaimonTypeMetadata::create(const folly::dynamic& obj) {
  return readMetadata(obj, 0);
}

PaimonSchema::PaimonSchema(
    int64_t schemaId,
    int32_t version,
    RowTypePtr rowType,
    PaimonTypeMetadata metadata,
    std::vector<int32_t> partitionKeys,
    std::vector<int32_t> primaryKeys,
    std::unordered_map<std::string, std::string> options)
    : schemaId_(schemaId),
      version_(version),
      rowType_(std::move(rowType)),
      metadata_(std::move(metadata)),
      partitionKeys_(std::move(partitionKeys)),
      primaryKeys_(std::move(primaryKeys)),
      options_(std::move(options)) {
  VELOX_USER_CHECK_GE(schemaId_, 0, "Paimon schema ID must be non-negative");
  VELOX_USER_CHECK(
      version_ >= 1 && version_ <= 3,
      "Unsupported Paimon schema version: {}",
      version_);
  VELOX_USER_CHECK_NOT_NULL(rowType_, "Missing Paimon schema type");
  metadata_.validate(rowType_, false);
  std::vector<column_index_t> path;
  indexFields(metadata_, path, fieldIndexPaths_);
  const auto validateKeys = [&](const auto& keys, std::string_view label) {
    std::unordered_set<int32_t> unique;
    for (auto id : keys) {
      VELOX_USER_CHECK(
          unique.insert(id).second,
          "Duplicate Paimon {} field ID: {}",
          label,
          id);
      VELOX_USER_CHECK_EQ(
          fieldIndexPath(id).size(),
          1,
          "Paimon {} must reference top-level fields",
          label);
    }
  };
  validateKeys(partitionKeys_, "partition key");
  validateKeys(primaryKeys_, "primary key");
  for (const auto& [key, _] : options_) {
    VELOX_USER_CHECK(
        !key.empty(), "Paimon schema option name must not be empty");
  }
  if (version_ == 1) {
    options_.try_emplace("bucket", "1");
  }
  if (version_ <= 2) {
    options_.try_emplace("file.format", "orc");
  }
}

const std::vector<column_index_t>& PaimonSchema::fieldIndexPath(
    int32_t fieldId) const {
  const auto it = fieldIndexPaths_.find(fieldId);
  VELOX_USER_CHECK(
      it != fieldIndexPaths_.end(),
      "Unknown Paimon field ID {} in schema {}",
      fieldId,
      schemaId_);
  return it->second;
}

TypePtr PaimonSchema::fieldType(int32_t fieldId) const {
  TypePtr type = rowType_;
  for (auto index : fieldIndexPath(fieldId)) {
    type = type->childAt(index);
  }
  return type;
}

const PaimonTypeMetadata& PaimonSchema::fieldMetadata(int32_t fieldId) const {
  const auto* metadata = &metadata_;
  for (auto index : fieldIndexPath(fieldId)) {
    metadata = &metadata->children[index];
  }
  return *metadata;
}

std::vector<int32_t> PaimonSchema::fieldIdPath(
    const common::Subfield& subfield) const {
  const auto& path = subfield.path();
  VELOX_USER_CHECK(!path.empty(), "Empty Paimon field path");
  const auto* root = path.front()->as<common::Subfield::NestedField>();
  VELOX_USER_CHECK_NOT_NULL(
      root, "Paimon field path must start with a column name");
  const auto index = rowType_->getChildIdxIfExists(root->name());
  VELOX_USER_CHECK(index.has_value(), "Unknown Paimon field: {}", root->name());
  return metadata_.children[*index].fieldIdPath(
      root->name(), rowType_->childAt(*index), subfield);
}

folly::dynamic PaimonSchema::serialize() const {
  folly::dynamic obj =
      folly::dynamic::object("schemaId", schemaId_)("schemaVersion", version_);
  obj["rowType"] = rowType_->serialize();
  obj["metadata"] = metadata_.serialize();
  obj["partitionKeys"] = folly::dynamic::array;
  obj["primaryKeys"] = folly::dynamic::array;
  for (auto id : partitionKeys_) {
    obj["partitionKeys"].push_back(id);
  }
  for (auto id : primaryKeys_) {
    obj["primaryKeys"].push_back(id);
  }
  obj["options"] = folly::dynamic::object;
  for (const auto& [key, value] : options_) {
    obj["options"][key] = value;
  }
  return obj;
}

PaimonSchemaPtr PaimonSchema::create(const folly::dynamic& obj) {
  VELOX_USER_CHECK(obj.isObject(), "Expected Paimon schema object");
  VELOX_USER_CHECK(obj.at("schemaId").isInt(), "Expected Paimon schema ID");
  auto type = ISerializable::deserialize<Type>(obj.at("rowType"));
  VELOX_USER_CHECK(type && type->isRow(), "Paimon schema must have a ROW type");
  VELOX_USER_CHECK(
      obj.at("options").isObject(), "Expected Paimon schema options object");
  std::unordered_map<std::string, std::string> options;
  for (const auto& [key, value] : obj.at("options").items()) {
    VELOX_USER_CHECK(
        key.isString() && value.isString(),
        "Paimon schema options must be strings");
    options.emplace(key.asString(), value.asString());
  }
  return std::make_shared<PaimonSchema>(
      obj.at("schemaId").asInt(),
      readInt32(obj.at("schemaVersion")),
      asRowType(type),
      PaimonTypeMetadata::create(obj.at("metadata")),
      readIds(obj.at("partitionKeys")),
      readIds(obj.at("primaryKeys")),
      std::move(options));
}

folly::dynamic serializeSubfield(const common::Subfield& subfield) {
  VELOX_USER_CHECK(subfield.valid(), "Invalid Paimon subfield path");
  folly::dynamic result = folly::dynamic::array;
  for (const auto& element : subfield.path()) {
    if (auto* field = element->as<common::Subfield::NestedField>()) {
      result.push_back(
          folly::dynamic::object("kind", "field")("name", field->name()));
    } else if (auto* index = element->as<common::Subfield::LongSubscript>()) {
      result.push_back(
          folly::dynamic::object("kind", "longSubscript")(
              "index", index->index()));
    } else if (auto* key = element->as<common::Subfield::StringSubscript>()) {
      result.push_back(
          folly::dynamic::object("kind", "stringSubscript")(
              "key", key->index()));
    } else {
      VELOX_USER_CHECK(
          element->is(common::SubfieldKind::kAllSubscripts),
          "Invalid Paimon path element");
      result.push_back(folly::dynamic::object("kind", "allSubscripts"));
    }
  }
  return result;
}

common::Subfield deserializeSubfield(const folly::dynamic& obj) {
  VELOX_USER_CHECK(
      obj.isArray() && !obj.empty(),
      "Expected non-empty Paimon subfield path array");
  VELOX_USER_CHECK_LE(
      obj.size(), kMaxTypeDepth + 1, "Paimon subfield path is too deep");
  std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
  for (const auto& element : obj) {
    VELOX_USER_CHECK(
        element.isObject() && element.at("kind").isString(),
        "Expected Paimon path element kind");
    const auto& kind = element.at("kind").getString();
    if (kind == "field") {
      VELOX_USER_CHECK(
          element.at("name").isString(), "Expected Paimon path field name");
      path.push_back(
          std::make_unique<common::Subfield::NestedField>(
              element.at("name").asString()));
    } else if (kind == "longSubscript") {
      VELOX_USER_CHECK(
          element.at("index").isInt(),
          "Expected Paimon path integer subscript");
      path.push_back(
          std::make_unique<common::Subfield::LongSubscript>(
              ISerializable::deserialize<long>(element.at("index"))));
    } else if (kind == "stringSubscript") {
      VELOX_USER_CHECK(
          element.at("key").isString(),
          "Expected Paimon path string subscript");
      path.push_back(
          std::make_unique<common::Subfield::StringSubscript>(
              element.at("key").asString()));
    } else if (kind == "allSubscripts") {
      path.push_back(std::make_unique<common::Subfield::AllSubscripts>());
    } else {
      VELOX_USER_FAIL("Unsupported Paimon path element kind: {}", kind);
    }
  }
  VELOX_USER_CHECK(
      path.front()->is(common::SubfieldKind::kNestedField),
      "Paimon field path must start with a column name");
  return common::Subfield(std::move(path));
}

} // namespace facebook::velox::connector::hive::paimon
