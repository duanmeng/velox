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
#include "velox/connectors/paimon/PaimonDataFileMeta.h"

#include <limits>

#include "velox/common/base/Exceptions.h"
#include "velox/common/serialization/Serializable.h"

namespace facebook::velox::connector::paimon {

std::string PaimonDataFile::layoutString(Layout layout) {
  switch (layout) {
    case Layout::kAppend:
      return "APPEND";
    case Layout::kKeyValue:
      return "KEY_VALUE";
  }
  VELOX_USER_FAIL(
      "Unsupported Paimon file layout: {}", static_cast<int>(layout));
}

PaimonDataFile::Layout PaimonDataFile::layoutFromString(
    const std::string& str) {
  if (str == "APPEND") {
    return Layout::kAppend;
  }
  if (str == "KEY_VALUE") {
    return Layout::kKeyValue;
  }
  VELOX_USER_FAIL("Unsupported Paimon file layout: {}", str);
}

void PaimonDataFile::validate() const {
  VELOX_USER_CHECK(!path.empty(), "Paimon file path must not be empty");
  const auto maxCount =
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  VELOX_USER_CHECK_LE(
      size, maxCount, "Paimon file size exceeds the wire range");
  VELOX_USER_CHECK_LE(
      rowCount, maxCount, "Paimon row count exceeds the wire range");
  VELOX_USER_CHECK_GE(level, 0, "Paimon file level must be nonnegative");
  if (rowCount != 0) {
    VELOX_USER_CHECK_LE(
        minSequenceNumber, maxSequenceNumber, "Invalid Paimon sequence range");
  }
  if (deleteRowCount) {
    VELOX_USER_CHECK_GE(*deleteRowCount, 0, "Negative Paimon delete row count");
    VELOX_USER_CHECK_LE(
        *deleteRowCount,
        static_cast<int64_t>(rowCount),
        "Paimon delete row count exceeds row count");
  }
  VELOX_USER_CHECK(
      schemaId.has_value() == fileFormat.has_value() &&
          schemaId.has_value() == layout.has_value(),
      "Paimon file schemaId, fileFormat and layout must be supplied together");
  if (schemaId) {
    VELOX_USER_CHECK_GE(
        *schemaId, 0, "Paimon file schema ID must be nonnegative");
    VELOX_USER_CHECK(
        *fileFormat != dwio::common::FileFormat::UNKNOWN,
        "Unknown Paimon file format");
    dwio::common::FileFormatName::toName(*fileFormat);
    layoutString(*layout);
  } else {
    VELOX_USER_CHECK(
        !properties && cacheable,
        "Paimon file access metadata requires schemaId, fileFormat and layout");
  }
  if (properties) {
    if (properties->fileSize) {
      VELOX_USER_CHECK_EQ(
          *properties->fileSize,
          static_cast<int64_t>(size),
          "Paimon file properties size does not match file size");
    }
    if (properties->readRangeHint) {
      VELOX_USER_CHECK_GE(
          *properties->readRangeHint,
          0,
          "Negative Paimon file read range hint");
    }
  }
  typeString(type);
  sourceString(source);
}

// static
std::string PaimonDataFile::typeString(Type type) {
  switch (type) {
    case Type::kData:
      return "DATA";
    case Type::kChangelog:
      return "CHANGELOG";
    default:
      VELOX_FAIL("Unknown PaimonDataFile::Type: {}", static_cast<int>(type));
  }
}

// static
PaimonDataFile::Type PaimonDataFile::typeFromString(const std::string& str) {
  if (str == "DATA") {
    return Type::kData;
  }
  if (str == "CHANGELOG") {
    return Type::kChangelog;
  }
  VELOX_FAIL("Unknown PaimonDataFile::Type: {}", str);
}

// static
std::string PaimonDataFile::sourceString(Source source) {
  switch (source) {
    case Source::kAppend:
      return "APPEND";
    case Source::kCompact:
      return "COMPACT";
    default:
      VELOX_FAIL(
          "Unknown PaimonDataFile::Source: {}", static_cast<int>(source));
  }
}

// static
PaimonDataFile::Source PaimonDataFile::sourceFromString(
    const std::string& str) {
  if (str == "APPEND") {
    return Source::kAppend;
  }
  if (str == "COMPACT") {
    return Source::kCompact;
  }
  VELOX_FAIL("Unknown PaimonDataFile::Source: {}", str);
}

std::string PaimonDataFile::toString() const {
  return fmt::format(
      "{{path={}, size={}, rows={}, level={}, type={}, source={}, "
      "deletionFile={}}}",
      path,
      size,
      rowCount,
      level,
      typeString(type),
      sourceString(source),
      deletionFile.has_value() ? deletionFile->toString() : "none");
}

folly::dynamic PaimonDataFile::serialize() const {
  validate();
  folly::dynamic obj = folly::dynamic::object;
  obj["filePath"] = path;
  obj["fileSize"] = size;
  obj["rowCount"] = rowCount;
  obj["level"] = level;
  obj["minSequenceNumber"] = minSequenceNumber;
  obj["maxSequenceNumber"] = maxSequenceNumber;
  obj["deleteRowCount"] =
      deleteRowCount ? folly::dynamic(*deleteRowCount) : nullptr;
  obj["creationTimeMs"] = creationTimeMs;
  obj["fileType"] = typeString(type);
  obj["sourceType"] = sourceString(source);
  if (deletionFile.has_value()) {
    obj["deletionFile"] = deletionFile->serialize();
  }
  if (schemaId) {
    obj["schemaId"] = *schemaId;
    obj["fileFormat"] = dwio::common::FileFormatName::toName(*fileFormat);
    obj["layout"] = layoutString(*layout);
    obj["cacheable"] = cacheable;
    obj["properties"] = properties ? properties->serialize() : nullptr;
  }
  return obj;
}

// static
PaimonDataFile PaimonDataFile::create(const folly::dynamic& obj) {
  PaimonDataFile file;
  file.path = obj["filePath"].asString();
  const auto size = obj.at("fileSize").asInt();
  const auto rowCount = obj.at("rowCount").asInt();
  VELOX_USER_CHECK_GE(size, 0, "Negative Paimon file size");
  VELOX_USER_CHECK_GE(rowCount, 0, "Negative Paimon row count");
  file.size = size;
  file.rowCount = rowCount;
  file.level = ISerializable::deserialize<int32_t>(obj.at("level"));
  file.minSequenceNumber = obj["minSequenceNumber"].asInt();
  file.maxSequenceNumber = obj["maxSequenceNumber"].asInt();
  file.deleteRowCount = ISerializable::deserialize<std::optional<int64_t>>(
      obj.at("deleteRowCount"));
  file.creationTimeMs = obj["creationTimeMs"].asInt();
  file.type = typeFromString(obj["fileType"].asString());
  file.source = sourceFromString(obj["sourceType"].asString());
  if (obj.count("deletionFile") > 0) {
    file.deletionFile = PaimonDeletionFile::create(obj["deletionFile"]);
  }
  if (obj.count("schemaId")) {
    file.schemaId = obj.at("schemaId").asInt();
  }
  if (obj.count("fileFormat")) {
    file.fileFormat =
        dwio::common::toFileFormat(obj.at("fileFormat").asString());
  }
  if (obj.count("layout")) {
    file.layout = layoutFromString(obj.at("layout").asString());
  }
  file.cacheable = obj.getDefault("cacheable", true).asBool();
  const auto& properties = obj.getDefault("properties", nullptr);
  if (!properties.isNull()) {
    file.properties = FileProperties::create(properties);
  }
  file.validate();
  return file;
}

} // namespace facebook::velox::connector::paimon
