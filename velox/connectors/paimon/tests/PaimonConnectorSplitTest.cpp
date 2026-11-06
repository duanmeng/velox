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
#include "velox/connectors/paimon/PaimonConnectorSplit.h"

#include <fmt/format.h>
#include <folly/json.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sstream>

#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox::connector::paimon;
using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

namespace {
using PartitionKeys =
    std::unordered_map<std::string, std::optional<std::string>>;
} // namespace

class PaimonConnectorSplitTest : public testing::Test {
 protected:
  static PaimonDataFile completeFile() {
    PaimonDataFile file;
    file.path = "data.nimble";
    file.size = 4096;
    file.rowCount = 100;
    file.schemaId = 17;
    file.fileFormat = FileFormat::NIMBLE;
    file.layout = PaimonDataFile::Layout::kAppend;
    return file;
  }

  const std::string kConnectorId{"test-connector"};
};

TEST_F(PaimonConnectorSplitTest, completeBuilderAndRegistryRoundTrip) {
  auto first = completeFile();
  first.minSequenceNumber = 3;
  first.maxSequenceNumber = 102;
  first.creationTimeMs = 1700000000;
  first.source = PaimonDataFile::Source::kCompact;
  first.cacheable = false;
  first.properties.emplace();
  first.properties->fileReadOps = {{"file", "first"}};
  first.deletionFile = PaimonDeletionFile{"deletes.bin", 4, 64, 2};
  auto second = completeFile();
  second.path = "data.parquet";
  second.schemaId = 18;
  second.fileFormat = FileFormat::PARQUET;
  second.deleteRowCount = 0;
  const auto split = PaimonConnectorSplitBuilder(
                         kConnectorId, 42, PaimonTableType::kAppendOnly)
                         .addFile(first)
                         .addFile(second)
                         .tableBucketNumber(-1)
                         .build();
  EXPECT_FALSE(split->isLegacy());
  EXPECT_FALSE(split->rawConvertible());
  const auto wire = split->serialize();
  EXPECT_EQ(wire["version"], PaimonConnectorSplit::kSerdeVersion);
  EXPECT_EQ(wire.count("fileFormat"), 0);
  PaimonConnectorSplit::registerSerDe();
  const auto base = ISerializable::deserialize<connector::ConnectorSplit>(
      folly::parseJson(folly::toJson(wire)));
  const auto copy = std::dynamic_pointer_cast<const PaimonConnectorSplit>(base);
  ASSERT_NE(copy, nullptr);
  EXPECT_EQ(copy->snapshotId(), 42);
  EXPECT_EQ(copy->tableBucketNumber(), -1);
  ASSERT_EQ(copy->dataFiles().size(), 2);
  EXPECT_EQ(copy->dataFiles()[0].serialize(), first.serialize());
  EXPECT_EQ(copy->dataFiles()[1].serialize(), second.serialize());
  EXPECT_EQ(copy->serialize(), wire);
  EXPECT_NO_THROW(copy->checkSchemaIds({17, 18}));
  EXPECT_NO_THROW(copy->checkSchemaIds({17, 18, 19}));
  VELOX_ASSERT_USER_THROW(
      copy->checkSchemaIds({17}), "Missing Paimon schema 18");
  VELOX_ASSERT_USER_THROW(copy->checkSchemaIds({}), "Missing Paimon schema 17");
}

TEST_F(PaimonConnectorSplitTest, versionAndMetadataMustAgree) {
  auto newBuilder = PaimonConnectorSplitBuilder(
      kConnectorId, 1, PaimonTableType::kAppendOnly);
  VELOX_ASSERT_USER_THROW(
      newBuilder.addFile("only-a-path", 10),
      "require complete PaimonDataFile metadata");
  auto complete = completeFile();
  newBuilder.addFile(complete);
  const auto original = newBuilder.build()->serialize();
  for (const auto version : {-1, 2}) {
    auto wire = original;
    wire["version"] = version;
    VELOX_ASSERT_USER_THROW(
        PaimonConnectorSplit::create(wire),
        "Unsupported Paimon split wire version");
  }
  auto wire = original;
  wire["fileFormat"] = "nimble";
  VELOX_ASSERT_USER_THROW(
      PaimonConnectorSplit::create(wire), "use per-file formats");
  wire.erase("version");
  VELOX_ASSERT_USER_THROW(
      PaimonConnectorSplit::create(wire),
      "version must match its file metadata");
  wire = original;
  for (const auto* key : {"schemaId", "fileFormat", "layout"}) {
    wire["dataFiles"][0].erase(key);
  }
  VELOX_ASSERT_USER_THROW(
      PaimonConnectorSplit::create(wire),
      "version must match its file metadata");
  for (const auto* key :
       {"snapshotId", "tableType", "rawConvertible", "dataFiles"}) {
    wire = original;
    wire.erase(key);
    EXPECT_ANY_THROW(PaimonConnectorSplit::create(wire)) << key;
  }
}

TEST_F(PaimonConnectorSplitTest, splitNumericBoundsAndLayout) {
  const auto original =
      PaimonConnectorSplitBuilder(kConnectorId, 0, PaimonTableType::kAppendOnly)
          .addFile(completeFile())
          .build()
          ->serialize();
  EXPECT_NO_THROW(PaimonConnectorSplit::create(original));
  auto wire = original;
  wire["snapshotId"] = -1;
  VELOX_ASSERT_USER_THROW(
      PaimonConnectorSplit::create(wire), "snapshot ID must be nonnegative");
  for (const auto bucket : {int64_t{-2}, int64_t{1} << 32}) {
    wire = original;
    wire["tableBucketNumber"] = bucket;
    EXPECT_THROW(PaimonConnectorSplit::create(wire), VeloxUserError);
  }
  wire = original;
  wire["dataFiles"][0]["layout"] = "KEY_VALUE";
  VELOX_ASSERT_USER_THROW(
      PaimonConnectorSplit::create(wire), "layout does not match table type");
  wire["tableType"] = "PRIMARY_KEY";
  EXPECT_NO_THROW(PaimonConnectorSplit::create(wire));
}

TEST_F(PaimonConnectorSplitTest, unknownDeletesDoNotProveRawEligibility) {
  auto file = completeFile();
  auto builder = PaimonConnectorSplitBuilder(
      kConnectorId, 1, PaimonTableType::kAppendOnly);
  builder.addFile(file).rawConvertible(true);
  VELOX_ASSERT_USER_THROW(
      builder.build(), "requires known zero deleteRowCount");
  builder.rawConvertible(false);
  EXPECT_EQ(builder.build()->dataFiles()[0].deleteRowCount, std::nullopt);
  file.deleteRowCount = 0;
  EXPECT_TRUE(
      PaimonConnectorSplitBuilder(kConnectorId, 1, PaimonTableType::kAppendOnly)
          .addFile(file)
          .rawConvertible(true)
          .build()
          ->rawConvertible());
}

TEST_F(PaimonConnectorSplitTest, legacyWireDoesNotInventSchemaMetadata) {
  const auto original =
      PaimonConnectorSplitBuilder(
          kConnectorId, 1, PaimonTableType::kAppendOnly, FileFormat::NIMBLE)
          .addFile("data.nimble", 1024)
          .build();
  const auto wire = original->serialize();
  EXPECT_EQ(wire.count("version"), 0);
  const auto copy = PaimonConnectorSplit::create(wire);
  EXPECT_TRUE(copy->isLegacy());
  EXPECT_FALSE(copy->dataFiles()[0].schemaId);
  EXPECT_EQ(copy->dataFiles()[0].deleteRowCount, 0);
  EXPECT_EQ(copy->serialize(), wire);
  VELOX_ASSERT_USER_THROW(
      copy->checkSchemaIds({17}), "Legacy Paimon input has no file schema IDs");
}

TEST_F(PaimonConnectorSplitTest, basic) {
  PaimonDataFile file0;
  file0.path = "s3://bucket/table/dt=2024-01-01/bucket-0/data-001.orc";
  file0.size = 1024;
  file0.rowCount = 100;
  file0.level = 0;
  file0.deleteRowCount = 0;

  PaimonDataFile file1;
  file1.path = "s3://bucket/table/dt=2024-01-01/bucket-0/data-002.orc";
  file1.size = 2048;
  file1.rowCount = 200;
  file1.level = 1;
  file1.deleteRowCount = 0;

  std::vector<PaimonDataFile> files{file0, file1};
  PartitionKeys partitionKeys{{"dt", "2024-01-01"}};

  auto split = std::make_shared<PaimonConnectorSplit>(
      kConnectorId,
      /*snapshotId=*/1,
      PaimonTableType::kPrimaryKey,
      FileFormat::DWRF,
      files,
      partitionKeys,
      0);

  EXPECT_EQ(split->snapshotId(), 1);
  EXPECT_EQ(split->tableType(), PaimonTableType::kPrimaryKey);
  EXPECT_EQ(split->fileFormat(), FileFormat::DWRF);
  EXPECT_TRUE(split->rawConvertible());
  EXPECT_EQ(split->tableBucketNumber(), 0);
  ASSERT_EQ(split->dataFiles().size(), 2);
  EXPECT_EQ(split->dataFiles()[0].path, files[0].path);
  EXPECT_EQ(split->dataFiles()[0].size, 1024);
  EXPECT_EQ(split->dataFiles()[1].path, files[1].path);
  EXPECT_EQ(split->dataFiles()[1].size, 2048);
  ASSERT_EQ(split->partitionKeys().count("dt"), 1);
  EXPECT_EQ(split->partitionKeys().at("dt"), "2024-01-01");
}

TEST_F(PaimonConnectorSplitTest, nimbleFormat) {
  PaimonDataFile file0;
  file0.path = "s3://bucket/table/data-001.nimble";
  file0.size = 4096;
  file0.rowCount = 100;
  file0.deleteRowCount = 0;
  std::vector<PaimonDataFile> files{file0};

  auto split = std::make_shared<PaimonConnectorSplit>(
      kConnectorId,
      /*snapshotId=*/1,
      PaimonTableType::kAppendOnly,
      FileFormat::NIMBLE,
      files,
      PartitionKeys{},
      std::nullopt);

  ASSERT_EQ(split->dataFiles().size(), 1);
  EXPECT_EQ(split->tableType(), PaimonTableType::kAppendOnly);
  EXPECT_EQ(split->fileFormat(), FileFormat::NIMBLE);
}

TEST_F(PaimonConnectorSplitTest, parquetFormat) {
  PaimonDataFile file0;
  file0.path = "s3://bucket/table/data-001.parquet";
  file0.size = 4096;
  file0.rowCount = 100;
  file0.deleteRowCount = 0;
  std::vector<PaimonDataFile> files{file0};

  auto split = std::make_shared<PaimonConnectorSplit>(
      kConnectorId,
      /*snapshotId=*/1,
      PaimonTableType::kAppendOnly,
      FileFormat::PARQUET,
      files,
      PartitionKeys{},
      std::nullopt);

  ASSERT_EQ(split->dataFiles().size(), 1);
  EXPECT_EQ(split->fileFormat(), FileFormat::PARQUET);
}

TEST_F(PaimonConnectorSplitTest, emptyFilesThrows) {
  std::vector<PaimonDataFile> noFiles;
  VELOX_ASSERT_THROW(
      std::make_shared<PaimonConnectorSplit>(
          kConnectorId,
          /*snapshotId=*/1,
          PaimonTableType::kAppendOnly,
          FileFormat::NIMBLE,
          noFiles,
          PartitionKeys{},
          std::nullopt),
      "PaimonConnectorSplit requires non-empty dataFiles");
}

TEST_F(PaimonConnectorSplitTest, rawConvertibleWithDeleteRowCountThrows) {
  PaimonDataFile file;
  file.path = "data-001.orc";
  file.size = 1024;
  file.rowCount = 100;
  file.level = 1;
  file.deleteRowCount = 5;

  std::vector<PaimonDataFile> files{file};
  VELOX_ASSERT_THROW(
      std::make_shared<PaimonConnectorSplit>(
          kConnectorId,
          /*snapshotId=*/1,
          PaimonTableType::kPrimaryKey,
          FileFormat::DWRF,
          files,
          PartitionKeys{},
          std::nullopt,
          /*rawConvertible=*/true),
      "rawConvertible split cannot have files with deleteRowCount > 0");
}

TEST_F(PaimonConnectorSplitTest, notRawConvertibleWithDeleteRowCount) {
  PaimonDataFile file;
  file.path = "data-001.orc";
  file.size = 1024;
  file.rowCount = 100;
  file.level = 1;
  file.deleteRowCount = 5;

  std::vector<PaimonDataFile> files{file};
  auto split = std::make_shared<PaimonConnectorSplit>(
      kConnectorId,
      /*snapshotId=*/1,
      PaimonTableType::kPrimaryKey,
      FileFormat::DWRF,
      files,
      PartitionKeys{},
      std::nullopt,
      /*rawConvertible=*/false);

  EXPECT_FALSE(split->rawConvertible());
  EXPECT_EQ(split->dataFiles()[0].deleteRowCount, 5);
}

TEST_F(PaimonConnectorSplitTest, toStringOutput) {
  PaimonDataFile f0;
  f0.path = "data-001.orc";
  f0.size = 1024;
  f0.rowCount = 100;
  f0.deleteRowCount = 0;

  PaimonDataFile f1;
  f1.path = "data-002.orc";
  f1.size = 2048;
  f1.rowCount = 200;
  f1.deleteRowCount = 0;
  f1.level = 1;

  PaimonDataFile f2;
  f2.path = "data-003.orc";
  f2.size = 512;
  f2.rowCount = 50;
  f2.deleteRowCount = 0;
  f2.level = 2;

  std::vector<PaimonDataFile> files{f0, f1, f2};

  auto split = std::make_shared<PaimonConnectorSplit>(
      kConnectorId,
      /*snapshotId=*/42,
      PaimonTableType::kPrimaryKey,
      FileFormat::DWRF,
      files,
      PartitionKeys{},
      std::nullopt);

  EXPECT_THAT(split->toString(), testing::HasSubstr("snapshot 42"));
  EXPECT_THAT(split->toString(), testing::HasSubstr("PRIMARY_KEY"));
  EXPECT_THAT(split->toString(), testing::HasSubstr(kConnectorId));
  EXPECT_THAT(split->toString(), testing::HasSubstr("data-001.orc"));
  EXPECT_THAT(split->toString(), testing::HasSubstr("data-003.orc"));
  EXPECT_THAT(split->toString(), testing::HasSubstr("size=1024"));
  EXPECT_THAT(split->toString(), testing::HasSubstr("rows=100"));
  EXPECT_THAT(split->toString(), testing::HasSubstr("level=1"));
  EXPECT_THAT(split->toString(), testing::HasSubstr("deletionFile=none"));
}

TEST_F(PaimonConnectorSplitTest, nullPartitionValue) {
  PaimonDataFile file0;
  file0.path = "data-001.orc";
  file0.size = 1024;
  file0.rowCount = 100;
  file0.deleteRowCount = 0;
  std::vector<PaimonDataFile> files{file0};
  PartitionKeys partitionKeys{{"country", std::nullopt}};

  auto split = std::make_shared<PaimonConnectorSplit>(
      kConnectorId,
      /*snapshotId=*/1,
      PaimonTableType::kAppendOnly,
      FileFormat::DWRF,
      files,
      partitionKeys,
      std::nullopt);

  ASSERT_EQ(split->partitionKeys().count("country"), 1);
  EXPECT_EQ(split->partitionKeys().at("country"), std::nullopt);
}

TEST_F(PaimonConnectorSplitTest, builder) {
  auto split = PaimonConnectorSplitBuilder(
                   kConnectorId,
                   /*snapshotId=*/5,
                   PaimonTableType::kPrimaryKey,
                   FileFormat::PARQUET)
                   .addFile("s3://bucket/data-001.parquet", 1024, 0)
                   .addFile("s3://bucket/data-002.parquet", 2048, 1)
                   .partitionKey("dt", "2024-01-01")
                   .partitionKey("country", std::nullopt)
                   .tableBucketNumber(3)
                   .build();

  EXPECT_EQ(split->snapshotId(), 5);
  EXPECT_EQ(split->fileFormat(), FileFormat::PARQUET);
  EXPECT_TRUE(split->rawConvertible());

  ASSERT_EQ(split->dataFiles().size(), 2);
  EXPECT_EQ(split->dataFiles()[0].path, "s3://bucket/data-001.parquet");
  EXPECT_EQ(split->dataFiles()[0].size, 1024);
  EXPECT_EQ(split->dataFiles()[1].path, "s3://bucket/data-002.parquet");
  EXPECT_EQ(split->dataFiles()[1].size, 2048);
  EXPECT_EQ(split->tableBucketNumber(), 3);
  EXPECT_EQ(split->partitionKeys().at("dt"), "2024-01-01");
  EXPECT_EQ(split->partitionKeys().at("country"), std::nullopt);
}

TEST_F(PaimonConnectorSplitTest, builderDefaults) {
  auto split = PaimonConnectorSplitBuilder(
                   kConnectorId,
                   /*snapshotId=*/1,
                   PaimonTableType::kAppendOnly,
                   FileFormat::NIMBLE)
                   .addFile("data.nimble", 512)
                   .build();

  ASSERT_EQ(split->dataFiles().size(), 1);
  EXPECT_EQ(split->fileFormat(), FileFormat::NIMBLE);
  EXPECT_EQ(split->tableBucketNumber(), std::nullopt);
  EXPECT_TRUE(split->partitionKeys().empty());
}

TEST_F(PaimonConnectorSplitTest, serializeRoundTrip) {
  PaimonDataFile meta;
  meta.path = "s3://bucket/data-001.parquet";
  meta.size = 4096;
  meta.rowCount = 1000;
  meta.level = 2;
  meta.minSequenceNumber = 10;
  meta.maxSequenceNumber = 20;
  meta.deleteRowCount = 5;
  meta.creationTimeMs = 1700000000;
  meta.source = PaimonDataFile::Source::kCompact;
  meta.deletionFile =
      PaimonDeletionFile{"s3://bucket/deletion-001.bin", 0, 128, 5};

  std::vector<PaimonDataFile> files{meta};
  PartitionKeys partitionKeys{{"dt", "2024-01-01"}, {"country", std::nullopt}};

  auto original = std::make_shared<PaimonConnectorSplit>(
      kConnectorId,
      /*snapshotId=*/42,
      PaimonTableType::kPrimaryKey,
      FileFormat::PARQUET,
      files,
      partitionKeys,
      /*tableBucketNumber=*/7,
      /*rawConvertible=*/false);

  auto serialized = original->serialize();
  auto deserialized = PaimonConnectorSplit::create(serialized);

  EXPECT_EQ(deserialized->connectorId, kConnectorId);
  EXPECT_EQ(deserialized->snapshotId(), 42);
  EXPECT_EQ(deserialized->tableType(), PaimonTableType::kPrimaryKey);
  EXPECT_EQ(deserialized->fileFormat(), FileFormat::PARQUET);
  EXPECT_FALSE(deserialized->rawConvertible());
  EXPECT_EQ(deserialized->tableBucketNumber(), 7);

  ASSERT_EQ(deserialized->dataFiles().size(), 1);
  const auto& file = deserialized->dataFiles()[0];
  EXPECT_EQ(file.path, "s3://bucket/data-001.parquet");
  EXPECT_EQ(file.size, 4096);
  EXPECT_EQ(file.rowCount, 1000);
  EXPECT_EQ(file.level, 2);
  EXPECT_EQ(file.minSequenceNumber, 10);
  EXPECT_EQ(file.maxSequenceNumber, 20);
  EXPECT_EQ(file.deleteRowCount, 5);
  EXPECT_EQ(file.creationTimeMs, 1700000000);
  EXPECT_EQ(file.source, PaimonDataFile::Source::kCompact);

  ASSERT_TRUE(file.deletionFile.has_value());
  EXPECT_EQ(file.deletionFile->path, "s3://bucket/deletion-001.bin");
  EXPECT_EQ(file.deletionFile->offset, 0);
  EXPECT_EQ(file.deletionFile->length, 128);
  EXPECT_EQ(file.deletionFile->cardinality, 5);

  ASSERT_EQ(deserialized->partitionKeys().count("dt"), 1);
  EXPECT_EQ(deserialized->partitionKeys().at("dt"), "2024-01-01");
  ASSERT_EQ(deserialized->partitionKeys().count("country"), 1);
  EXPECT_EQ(deserialized->partitionKeys().at("country"), std::nullopt);

  ASSERT_EQ(deserialized->dataFiles().size(), 1);
  EXPECT_EQ(deserialized->dataFiles()[0].path, "s3://bucket/data-001.parquet");
  EXPECT_EQ(deserialized->dataFiles()[0].size, 4096);
}

TEST_F(PaimonConnectorSplitTest, paimonTableTypeStringAndParse) {
  EXPECT_EQ(paimonTableTypeString(PaimonTableType::kAppendOnly), "APPEND_ONLY");
  EXPECT_EQ(paimonTableTypeString(PaimonTableType::kPrimaryKey), "PRIMARY_KEY");

  EXPECT_EQ(
      paimonTableTypeFromString("APPEND_ONLY"), PaimonTableType::kAppendOnly);
  EXPECT_EQ(
      paimonTableTypeFromString("PRIMARY_KEY"), PaimonTableType::kPrimaryKey);

  VELOX_ASSERT_THROW(
      paimonTableTypeFromString("UNKNOWN"), "Unknown PaimonTableType: UNKNOWN");
}

TEST_F(PaimonConnectorSplitTest, tableTypeStreamAndFormat) {
  {
    std::ostringstream os;
    os << PaimonTableType::kAppendOnly;
    EXPECT_EQ(os.str(), "APPEND_ONLY");
  }
  {
    std::ostringstream os;
    os << PaimonTableType::kPrimaryKey;
    EXPECT_EQ(os.str(), "PRIMARY_KEY");
  }

  EXPECT_EQ(fmt::format("{}", PaimonTableType::kAppendOnly), "APPEND_ONLY");
  EXPECT_EQ(fmt::format("{}", PaimonTableType::kPrimaryKey), "PRIMARY_KEY");
}
