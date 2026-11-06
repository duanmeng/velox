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

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <limits>
#include <sstream>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox::connector::paimon;
using namespace facebook::velox;

namespace {
PaimonDataFile completeFile() {
  PaimonDataFile file;
  file.path = "data.nimble";
  file.size = 4096;
  file.rowCount = 100;
  file.schemaId = 17;
  file.fileFormat = dwio::common::FileFormat::NIMBLE;
  file.layout = PaimonDataFile::Layout::kAppend;
  return file;
}
} // namespace

TEST(PaimonDataFileTest, completeMetadataAndAccessDescription) {
  auto file = completeFile();
  file.cacheable = false;
  file.properties.emplace();
  file.properties->fileSize = file.size;
  file.properties->modificationTime = 1700000000;
  file.properties->readRangeHint = 128;
  file.properties->extraFileInfo = std::make_shared<std::string>("descriptor");
  file.properties->fileReadOps = {{"trace", "scan-17"}};

  auto wire = file.serialize();
  EXPECT_EQ(wire["schemaId"], 17);
  EXPECT_EQ(wire["fileFormat"], "nimble");
  EXPECT_EQ(wire["layout"], "APPEND");
  EXPECT_TRUE(wire["deleteRowCount"].isNull());
  const auto copy = PaimonDataFile::create(wire);
  EXPECT_EQ(copy.schemaId, 17);
  EXPECT_EQ(copy.fileFormat, dwio::common::FileFormat::NIMBLE);
  EXPECT_EQ(copy.layout, PaimonDataFile::Layout::kAppend);
  EXPECT_FALSE(copy.cacheable);
  ASSERT_TRUE(copy.properties);
  EXPECT_EQ(copy.properties->fileSize, 4096);
  EXPECT_EQ(copy.properties->modificationTime, 1700000000);
  EXPECT_EQ(copy.properties->readRangeHint, 128);
  ASSERT_TRUE(copy.properties->extraFileInfo);
  EXPECT_EQ(*copy.properties->extraFileInfo, "descriptor");
  EXPECT_EQ(copy.properties->fileReadOps.at("trace"), "scan-17");
  EXPECT_EQ(copy.properties->ioStatistics, nullptr);
  EXPECT_EQ(copy.serialize(), wire);
}

TEST(PaimonDataFileTest, unknownZeroAndPositiveDeletes) {
  auto file = completeFile();
  for (const auto count :
       {std::optional<int64_t>{},
        std::optional<int64_t>{0},
        std::optional<int64_t>{3}}) {
    file.deleteRowCount = count;
    auto wire = file.serialize();
    EXPECT_EQ(wire["deleteRowCount"].isNull(), !count.has_value());
    EXPECT_EQ(PaimonDataFile::create(wire).deleteRowCount, count);
  }
  auto wire = file.serialize();
  wire.erase("deleteRowCount");
  EXPECT_ANY_THROW(PaimonDataFile::create(wire));
}

TEST(PaimonDataFileTest, legacyWireIsPreserved) {
  const auto wire = folly::dynamic::object("filePath", "legacy.orc")(
      "fileSize", 1024)("rowCount", 100)("level", 0)("minSequenceNumber", -1)(
      "maxSequenceNumber", -1)("deleteRowCount", 0)("creationTimeMs", 0)(
      "fileType", "DATA")("sourceType", "APPEND");
  const auto file = PaimonDataFile::create(wire);
  EXPECT_FALSE(file.schemaId);
  EXPECT_FALSE(file.fileFormat);
  EXPECT_FALSE(file.layout);
  EXPECT_EQ(file.deleteRowCount, 0);
  EXPECT_EQ(file.serialize(), wire);
}

TEST(PaimonDataFileTest, malformedNumericMetadata) {
  const auto original = completeFile().serialize();
  for (const auto* key :
       {"fileSize", "rowCount", "level", "schemaId", "deleteRowCount"}) {
    auto wire = original;
    wire[key] = -1;
    EXPECT_THROW(PaimonDataFile::create(wire), VeloxUserError) << key;
  }
  auto wire = original;
  wire["level"] = int64_t{1} << 32;
  EXPECT_THROW(PaimonDataFile::create(wire), VeloxUserError);
  wire = original;
  wire["deleteRowCount"] = 101;
  VELOX_ASSERT_USER_THROW(PaimonDataFile::create(wire), "exceeds row count");
  wire = original;
  wire["minSequenceNumber"] = 2;
  wire["maxSequenceNumber"] = 1;
  VELOX_ASSERT_USER_THROW(
      PaimonDataFile::create(wire), "Invalid Paimon sequence range");

  auto file = completeFile();
  file.size = std::numeric_limits<uint64_t>::max();
  VELOX_ASSERT_USER_THROW(file.serialize(), "file size exceeds the wire range");
  file = completeFile();
  file.rowCount = std::numeric_limits<uint64_t>::max();
  VELOX_ASSERT_USER_THROW(file.serialize(), "row count exceeds the wire range");
  file = completeFile();
  file.size = file.rowCount = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(PaimonDataFile::create(file.serialize()).size, file.size);
  EXPECT_EQ(PaimonDataFile::create(file.serialize()).rowCount, file.rowCount);
}

TEST(PaimonDataFileTest, zeroRowsAndSequenceSentinel) {
  auto file = completeFile();
  file.rowCount = 0;
  file.minSequenceNumber = 0;
  file.maxSequenceNumber = -1;
  EXPECT_EQ(PaimonDataFile::create(file.serialize()).rowCount, 0);
  file.rowCount = 10;
  file.minSequenceNumber = file.maxSequenceNumber = -1;
  EXPECT_EQ(PaimonDataFile::create(file.serialize()).minSequenceNumber, -1);
}

TEST(PaimonDataFileTest, incompleteOrUnsupportedFileDescription) {
  const auto original = completeFile().serialize();
  for (const auto* key : {"schemaId", "fileFormat", "layout"}) {
    auto wire = original;
    wire.erase(key);
    VELOX_ASSERT_USER_THROW(
        PaimonDataFile::create(wire), "must be supplied together");
  }
  auto wire = original;
  wire["fileFormat"] = "not-a-format";
  VELOX_ASSERT_USER_THROW(
      PaimonDataFile::create(wire), "Unknown Paimon file format");
  wire = original;
  wire["layout"] = "THIN_KEY_VALUE";
  VELOX_ASSERT_USER_THROW(
      PaimonDataFile::create(wire), "Unsupported Paimon file layout");
  wire = original;
  wire["filePath"] = "";
  VELOX_ASSERT_USER_THROW(
      PaimonDataFile::create(wire), "path must not be empty");

  auto file = completeFile();
  file.properties.emplace();
  file.properties->fileSize = 1;
  VELOX_ASSERT_USER_THROW(file.serialize(), "does not match file size");
  file.properties->fileSize = file.size;
  file.properties->readRangeHint = -1;
  VELOX_ASSERT_USER_THROW(
      file.serialize(), "Negative Paimon file read range hint");
}

TEST(PaimonDataFileTest, serializeRoundTrip) {
  PaimonDataFile file;
  file.path = "data.parquet";
  file.size = 4096;
  file.rowCount = 500;
  file.level = 1;
  file.minSequenceNumber = 5;
  file.maxSequenceNumber = 15;
  file.deleteRowCount = 3;
  file.creationTimeMs = 1700000000;
  file.type = PaimonDataFile::Type::kChangelog;
  file.source = PaimonDataFile::Source::kCompact;
  file.deletionFile = PaimonDeletionFile{"del.bin", 0, 64, 2};

  auto serialized = file.serialize();
  auto deserialized = PaimonDataFile::create(serialized);

  EXPECT_EQ(deserialized.path, "data.parquet");
  EXPECT_EQ(deserialized.size, 4096);
  EXPECT_EQ(deserialized.rowCount, 500);
  EXPECT_EQ(deserialized.level, 1);
  EXPECT_EQ(deserialized.minSequenceNumber, 5);
  EXPECT_EQ(deserialized.maxSequenceNumber, 15);
  EXPECT_EQ(deserialized.deleteRowCount, 3);
  EXPECT_EQ(deserialized.creationTimeMs, 1700000000);
  EXPECT_EQ(deserialized.type, PaimonDataFile::Type::kChangelog);
  EXPECT_EQ(deserialized.source, PaimonDataFile::Source::kCompact);
  ASSERT_TRUE(deserialized.deletionFile.has_value());
  EXPECT_EQ(deserialized.deletionFile->path, "del.bin");
  EXPECT_EQ(deserialized.deletionFile->offset, 0);
  EXPECT_EQ(deserialized.deletionFile->length, 64);
  EXPECT_EQ(deserialized.deletionFile->cardinality, 2);
}

TEST(PaimonDataFileTest, serializeNoDeletionFile) {
  PaimonDataFile file;
  file.path = "data.orc";
  file.size = 1024;

  auto serialized = file.serialize();
  auto deserialized = PaimonDataFile::create(serialized);

  EXPECT_EQ(deserialized.path, "data.orc");
  EXPECT_EQ(deserialized.size, 1024);
  EXPECT_FALSE(deserialized.deletionFile.has_value());
}

TEST(PaimonDataFileTest, serializeDefaultValues) {
  PaimonDataFile file;
  file.path = "data.orc";

  auto serialized = file.serialize();
  auto deserialized = PaimonDataFile::create(serialized);

  EXPECT_EQ(deserialized.path, "data.orc");
  EXPECT_EQ(deserialized.size, 0);
  EXPECT_EQ(deserialized.rowCount, 0);
  EXPECT_EQ(deserialized.level, 0);
  EXPECT_EQ(deserialized.minSequenceNumber, 0);
  EXPECT_EQ(deserialized.maxSequenceNumber, 0);
  EXPECT_EQ(deserialized.deleteRowCount, std::nullopt);
  EXPECT_EQ(deserialized.creationTimeMs, 0);
  EXPECT_EQ(deserialized.type, PaimonDataFile::Type::kData);
  EXPECT_EQ(deserialized.source, PaimonDataFile::Source::kAppend);
  EXPECT_FALSE(deserialized.deletionFile.has_value());
}

TEST(PaimonDataFileTest, toString) {
  PaimonDataFile file;
  file.path = "data.orc";
  file.size = 1024;
  file.rowCount = 100;
  file.level = 0;

  auto str = file.toString();
  EXPECT_THAT(str, testing::HasSubstr("data.orc"));
  EXPECT_THAT(str, testing::HasSubstr("size=1024"));
  EXPECT_THAT(str, testing::HasSubstr("rows=100"));
  EXPECT_THAT(str, testing::HasSubstr("level=0"));
  EXPECT_THAT(str, testing::HasSubstr("type=DATA"));
  EXPECT_THAT(str, testing::HasSubstr("source=APPEND"));
  EXPECT_THAT(str, testing::HasSubstr("deletionFile=none"));
}

TEST(PaimonDataFileTest, toStringChangelog) {
  PaimonDataFile file;
  file.path = "changelog.orc";
  file.size = 2048;
  file.rowCount = 200;
  file.level = 1;
  file.type = PaimonDataFile::Type::kChangelog;
  file.source = PaimonDataFile::Source::kCompact;
  file.deletionFile = PaimonDeletionFile{"del.bin", 0, 64, 2};

  auto str = file.toString();
  EXPECT_THAT(str, testing::HasSubstr("type=CHANGELOG"));
  EXPECT_THAT(str, testing::HasSubstr("source=COMPACT"));
  EXPECT_THAT(str, testing::HasSubstr("del.bin"));
  EXPECT_THAT(str, testing::HasSubstr("cardinality=2"));
}

TEST(PaimonDataFileTest, defaultValues) {
  PaimonDataFile file;
  EXPECT_TRUE(file.path.empty());
  EXPECT_EQ(file.size, 0);
  EXPECT_EQ(file.rowCount, 0);
  EXPECT_EQ(file.level, 0);
  EXPECT_EQ(file.minSequenceNumber, 0);
  EXPECT_EQ(file.maxSequenceNumber, 0);
  EXPECT_EQ(file.deleteRowCount, std::nullopt);
  EXPECT_EQ(file.creationTimeMs, 0);
  EXPECT_EQ(file.type, PaimonDataFile::Type::kData);
  EXPECT_EQ(file.source, PaimonDataFile::Source::kAppend);
  EXPECT_FALSE(file.deletionFile.has_value());
}

TEST(PaimonDataFileTest, typeStringAndParse) {
  EXPECT_EQ(PaimonDataFile::typeString(PaimonDataFile::Type::kData), "DATA");
  EXPECT_EQ(
      PaimonDataFile::typeString(PaimonDataFile::Type::kChangelog),
      "CHANGELOG");

  EXPECT_EQ(
      PaimonDataFile::typeFromString("DATA"), PaimonDataFile::Type::kData);
  EXPECT_EQ(
      PaimonDataFile::typeFromString("CHANGELOG"),
      PaimonDataFile::Type::kChangelog);

  VELOX_ASSERT_THROW(
      PaimonDataFile::typeFromString("UNKNOWN"),
      "Unknown PaimonDataFile::Type: UNKNOWN");
}

TEST(PaimonDataFileTest, sourceStringAndParse) {
  EXPECT_EQ(
      PaimonDataFile::sourceString(PaimonDataFile::Source::kAppend), "APPEND");
  EXPECT_EQ(
      PaimonDataFile::sourceString(PaimonDataFile::Source::kCompact),
      "COMPACT");

  EXPECT_EQ(
      PaimonDataFile::sourceFromString("APPEND"),
      PaimonDataFile::Source::kAppend);
  EXPECT_EQ(
      PaimonDataFile::sourceFromString("COMPACT"),
      PaimonDataFile::Source::kCompact);

  VELOX_ASSERT_THROW(
      PaimonDataFile::sourceFromString("UNKNOWN"),
      "Unknown PaimonDataFile::Source: UNKNOWN");
}

TEST(PaimonDataFileTest, typeStreamAndFormat) {
  {
    std::ostringstream os;
    os << PaimonDataFile::Type::kData;
    EXPECT_EQ(os.str(), "DATA");
  }
  {
    std::ostringstream os;
    os << PaimonDataFile::Type::kChangelog;
    EXPECT_EQ(os.str(), "CHANGELOG");
  }

  EXPECT_EQ(fmt::format("{}", PaimonDataFile::Type::kData), "DATA");
  EXPECT_EQ(fmt::format("{}", PaimonDataFile::Type::kChangelog), "CHANGELOG");
}

TEST(PaimonDataFileTest, sourceStreamAndFormat) {
  {
    std::ostringstream os;
    os << PaimonDataFile::Source::kAppend;
    EXPECT_EQ(os.str(), "APPEND");
  }
  {
    std::ostringstream os;
    os << PaimonDataFile::Source::kCompact;
    EXPECT_EQ(os.str(), "COMPACT");
  }

  EXPECT_EQ(fmt::format("{}", PaimonDataFile::Source::kAppend), "APPEND");
  EXPECT_EQ(fmt::format("{}", PaimonDataFile::Source::kCompact), "COMPACT");
}
