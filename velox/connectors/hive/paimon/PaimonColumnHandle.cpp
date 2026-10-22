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
#include "velox/connectors/hive/paimon/PaimonColumnHandle.h"

#include <fmt/ranges.h>

namespace facebook::velox::connector::hive::paimon {

PaimonColumnHandle::PaimonColumnHandle(
    std::string name,
    ColumnType columnType,
    TypePtr dataType,
    int64_t schemaId,
    PaimonTypeMetadata metadata,
    std::vector<common::Subfield> requiredSubfields)
    : name_(std::move(name)),
      columnType_(columnType),
      dataType_(std::move(dataType)),
      schemaId_(schemaId),
      metadata_(std::move(metadata)),
      requiredSubfields_(std::move(requiredSubfields)) {
  VELOX_USER_CHECK(!name_.empty(), "Paimon column name must not be empty");
  VELOX_USER_CHECK_GE(schemaId_, 0, "Paimon schema ID must be non-negative");
  VELOX_USER_CHECK(
      columnType_ == ColumnType::kRegular ||
          columnType_ == ColumnType::kPartitionKey,
      "Unsupported Paimon column role: {}",
      static_cast<int>(columnType_));
  metadata_.validate(dataType_, true);
  for (const auto& subfield : requiredSubfields_) {
    fieldIdPath(subfield);
  }
}

bool PaimonColumnHandle::isPartitionDateValueDaysSinceEpoch() const {
  VELOX_UNSUPPORTED("Paimon partition values require typed binding");
}

std::string PaimonColumnHandle::toString() const {
  return fmt::format(
      "PaimonColumnHandle [name: {}, columnType: {}, schemaId: {}, fieldId: {}, "
      "dataType: {}, logicalType: {}, nullable: {}, requiredSubfields: [{}]]",
      name_,
      columnType_,
      schemaId_,
      fieldId(),
      dataType_->toString(),
      metadata_.logicalType,
      metadata_.nullable,
      fmt::join(requiredSubfields_, ", "));
}

folly::dynamic PaimonColumnHandle::serialize() const {
  auto obj = ColumnHandle::serializeBase("PaimonColumnHandle");
  obj["version"] = kSerdeVersion;
  obj["columnName"] = name_;
  obj["columnType"] = columnTypeName(columnType_);
  obj["dataType"] = dataType_->serialize();
  obj["schemaId"] = schemaId_;
  obj["metadata"] = metadata_.serialize();
  obj["requiredSubfields"] = folly::dynamic::array;
  for (const auto& subfield : requiredSubfields_) {
    obj["requiredSubfields"].push_back(serializeSubfield(subfield));
  }
  return obj;
}

ColumnHandlePtr PaimonColumnHandle::create(const folly::dynamic& obj) {
  VELOX_USER_CHECK(obj.isObject(), "Expected PaimonColumnHandle object");
  for (const auto* key : {"columnName", "columnType"}) {
    VELOX_USER_CHECK(obj.at(key).isString(), "Expected Paimon {} string", key);
  }
  VELOX_USER_CHECK(
      obj.at("version").isInt() && obj.at("version").asInt() == kSerdeVersion,
      "Unsupported PaimonColumnHandle version");
  VELOX_USER_CHECK(obj.at("schemaId").isInt(), "Expected Paimon schema ID");
  VELOX_USER_CHECK(
      obj.at("requiredSubfields").isArray(),
      "Expected Paimon required subfields array");
  std::vector<common::Subfield> requiredSubfields;
  for (const auto& subfield : obj.at("requiredSubfields")) {
    requiredSubfields.push_back(deserializeSubfield(subfield));
  }
  return std::make_shared<PaimonColumnHandle>(
      obj.at("columnName").asString(),
      columnTypeFromName(obj.at("columnType").asString()),
      ISerializable::deserialize<Type>(obj.at("dataType")),
      obj.at("schemaId").asInt(),
      PaimonTypeMetadata::create(obj.at("metadata")),
      std::move(requiredSubfields));
}

void PaimonColumnHandle::registerSerDe() {
  DeserializationRegistryForSharedPtr().Register("PaimonColumnHandle", create);
}

} // namespace facebook::velox::connector::hive::paimon
