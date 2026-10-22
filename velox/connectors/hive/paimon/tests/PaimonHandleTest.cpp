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

#include <limits>
#include <set>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/FileDataSource.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::velox::connector::hive::paimon {
namespace {

using common::Subfield;
using ColumnType = FileColumnHandle::ColumnType;
using ReadMode = PaimonTableHandle::ReadMode;

std::vector<Subfield> subfields(std::initializer_list<std::string> paths) {
  std::vector<Subfield> result;
  for (const auto& path : paths) {
    result.emplace_back(path);
  }
  return result;
}

class PreparedFileDataSource : public FileDataSource {
 public:
  using FileDataSource::FileDataSource;

  const common::ScanSpec& scanSpec() const {
    return *scanSpec_;
  }

  const RowTypePtr& readerOutputType() const {
    return readerOutputType_;
  }

  const auto& partitionColumns() const {
    return partitionKeys_;
  }

  const exec::ExprSet* filterExprSet() const {
    return remainingFilterExprSet();
  }
};

class PaimonHandleTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    Type::registerSerDe();
    common::Filter::registerSerDe();
    core::ITypedExpr::registerSerDe();
    PaimonColumnHandle::registerSerDe();
    PaimonTableHandle::registerSerDe();
  }

  static PaimonSchemaPtr targetSchema() {
    return std::make_shared<PaimonSchema>(
        20,
        3,
        ROW(
            {{"id", BIGINT()},
             {"payload",
              ROW({{"a", BIGINT()}, {"b", BIGINT()}, {"c", BIGINT()}})},
             {"p", VARCHAR()},
             {"aux", BIGINT()},
             {"arr", ARRAY(BIGINT())}}),
        PaimonTypeMetadata{
            "ROW",
            false,
            std::nullopt,
            {},
            {{"BIGINT", false, 7},
             {"ROW",
              true,
              8,
              {},
              {{"BIGINT", true, 9},
               {"BIGINT", false, 10},
               {"BIGINT", true, 15}}},
             {"VARCHAR", true, 11, {20}},
             {"BIGINT", true, 12},
             {"ARRAY", true, 13, {}, {{"BIGINT", false}}}}},
        std::vector<int32_t>{11},
        std::vector<int32_t>{},
        std::unordered_map<std::string, std::string>{
            {"file.format", "nimble"}, {"nested-field", "false"}});
  }

  static PaimonTableHandlePtr table(
      common::SubfieldFilters filters = {},
      core::TypedExprPtr remainingFilter = nullptr,
      std::vector<PaimonColumnHandlePtr> filterColumns = {},
      std::unordered_map<std::string, std::string> options = {},
      double sampleRate = 1.0) {
    return std::make_shared<PaimonTableHandle>(
        "paimon",
        "orders",
        "/warehouse/orders",
        PaimonTableType::kAppendOnly,
        20,
        std::vector<PaimonSchemaPtr>{targetSchema()},
        std::move(filters),
        std::move(remainingFilter),
        ReadMode::kSnapshot,
        std::move(options),
        std::move(filterColumns),
        sampleRate,
        "test_db");
  }

  static std::set<std::string> filterColumnNames(const FileTableHandle& table) {
    std::set<std::string> result;
    for (const auto& column : table.filterColumnHandles()) {
      result.insert(column->name());
    }
    return result;
  }

  void checkFilePreparation(bool zeroOutput, bool prune = true) {
    const auto schema = targetSchema();
    common::SubfieldFilters filters;
    auto range = std::make_shared<common::BigintRange>(1, 10, false);
    filters.emplace(Subfield("payload.a"), range);
    filters.emplace(
        Subfield("p"),
        std::make_shared<common::BytesValues>(
            std::vector<std::string>{"east"}, false));
    auto remaining = parseExpr("aux % 7 = 1", schema->rowType());
    const auto handle = table(std::move(filters), remaining);
    // Run the real FileDataSource preparation after registry deserialization.
    const auto copy = ISerializable::deserialize<PaimonTableHandle>(
        handle->serialize(), pool());
    auto outputType = ROW({}, {});
    ColumnHandleMap assignments;
    if (!zeroOutput) {
      outputType =
          ROW({{"output_payload", schema->rowType()->findChild("payload")}});
      assignments.emplace(
          "output_payload",
          copy->columnHandle(
              8, prune ? subfields({"payload.b"}) : subfields({})));
    }
    copy->validateAssignments(outputType, assignments);
    auto queryCtx = core::QueryCtx::create();
    auto config = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    auto context = ConnectorQueryCtx::Builder()
                       .operatorPool(pool())
                       .connectorPool(rootPool_.get())
                       .sessionProperties(config.get())
                       .expressionEvaluator(
                           std::make_unique<exec::SimpleExpressionEvaluator>(
                               queryCtx.get(), pool()))
                       .queryId("PaimonHandleTest")
                       .taskId("PaimonHandleTest")
                       .planNodeId("scan")
                       .build();
    PreparedFileDataSource source(
        outputType,
        copy,
        assignments,
        nullptr,
        nullptr,
        context.get(),
        std::make_shared<HiveConfig>(config));
    ASSERT_NE(source.filterExprSet(), nullptr);
    EXPECT_TRUE(source.readerOutputType()->containsChild("aux"));
    EXPECT_EQ(source.readerOutputType()->size(), zeroOutput ? 1 : 2);
    ASSERT_EQ(source.partitionColumns().count("p"), 1);
    EXPECT_TRUE(source.partitionColumns().at("p")->isPartitionKey());
    EXPECT_EQ(source.partitionColumns().at("p")->dataType(), VARCHAR());
    ASSERT_EQ(source.getFilters()->size(), 2);
    EXPECT_TRUE(source.getFilters()->at(Subfield("payload.a"))->testInt64(5));
    EXPECT_FALSE(source.getFilters()->at(Subfield("payload.a"))->testInt64(20));
    EXPECT_EQ(
        source.scanSpec().childByName("p")->filter(),
        source.getFilters()->at(Subfield("p")).get());
    const auto* payload = source.scanSpec().childByName("payload");
    ASSERT_NE(payload, nullptr);
    EXPECT_FALSE(payload->childByName("a")->isConstant());
    EXPECT_EQ(payload->projectOut(), !zeroOutput);
    if (!zeroOutput) {
      EXPECT_FALSE(payload->childByName("b")->isConstant());
      EXPECT_EQ(payload->childByName("c")->isConstant(), prune);
      if (prune) {
        EXPECT_TRUE(payload->childByName("c")->constantValue()->isNullAt(0));
      }
    }
    // The shared preparation copies filter references, not filter payloads.
    EXPECT_EQ(
        source.getFilters()->at(Subfield("payload.a")).get(),
        copy->subfieldFilters().at(Subfield("payload.a")).get());
  }
};

TEST_F(PaimonHandleTest, columnRoundTripAndFileInterface) {
  const auto column =
      table()->columnHandle(8, subfields({"payload.b", "payload.a"}));
  const auto copy =
      ISerializable::deserialize<PaimonColumnHandle>(column->serialize());
  const FileColumnHandle& fileColumn = *copy;
  EXPECT_EQ(copy->serialize(), column->serialize());
  EXPECT_EQ(copy->toString(), column->toString());
  EXPECT_EQ(fileColumn.name(), "payload");
  EXPECT_EQ(fileColumn.columnType(), ColumnType::kRegular);
  EXPECT_EQ(copy->schemaId(), 20);
  EXPECT_EQ(copy->fieldId(), 8);
  EXPECT_EQ(copy->metadata(), targetSchema()->fieldMetadata(8));
  EXPECT_EQ(fileColumn.schemaType(), fileColumn.dataType());
  EXPECT_EQ(fileColumn.requiredSubfields().size(), 2);
  EXPECT_EQ(
      copy->fieldIdPath(Subfield("payload.b")), (std::vector<int32_t>{8, 10}));
  EXPECT_TRUE(fileColumn.extractions().empty());
  EXPECT_FALSE(fileColumn.postProcessor());
  EXPECT_TRUE(table()->columnHandle(11)->isPartitionKey());
  VELOX_ASSERT_THROW(
      table()->columnHandle(11)->isPartitionDateValueDaysSinceEpoch(),
      "Paimon partition values require typed binding");
}

TEST_F(PaimonHandleTest, columnValidation) {
  auto obj = table()->columnHandle(8)->serialize();
  obj["requiredSubfields"] =
      folly::dynamic::array(serializeSubfield(Subfield("alias.a")));
  VELOX_ASSERT_THROW(
      PaimonColumnHandle::create(obj), "must start with payload");
  obj["requiredSubfields"] =
      folly::dynamic::array(serializeSubfield(Subfield("payload.missing")));
  VELOX_ASSERT_THROW(
      PaimonColumnHandle::create(obj), "Unknown Paimon subfield");
  obj["requiredSubfields"] =
      folly::dynamic::array(serializeSubfield(Subfield("payload.a[1]")));
  VELOX_ASSERT_THROW(
      PaimonColumnHandle::create(obj), "Invalid Paimon subfield path");
  for (auto role :
       {ColumnType::kRowIndex, ColumnType::kRowId, ColumnType::kSynthesized}) {
    VELOX_ASSERT_THROW(
        PaimonColumnHandle("id", role, BIGINT(), 20, {"BIGINT", false, 7}),
        "Unsupported Paimon column role");
  }
  VELOX_ASSERT_THROW(
      PaimonColumnHandle(
          "", ColumnType::kRegular, BIGINT(), 20, {"BIGINT", false, 7}),
      "column name must not be empty");
  VELOX_ASSERT_THROW(
      PaimonColumnHandle(
          "id", ColumnType::kRegular, BIGINT(), -1, {"BIGINT", false, 7}),
      "schema ID must be non-negative");
}

TEST_F(PaimonHandleTest, tableRoundTripAndFileInterface) {
  common::SubfieldFilters filters;
  filters.emplace(
      Subfield("payload.a"),
      std::make_shared<common::BigintRange>(1, 10, true));
  auto remaining = parseExpr("aux % 5 = 0", targetSchema()->rowType());
  const auto handle = table(
      std::move(filters),
      remaining,
      {table()->columnHandle(11)},
      {{"nested-field", "true"}},
      0.25);
  const auto copy = ISerializable::deserialize<PaimonTableHandle>(
      handle->serialize(), pool());
  const FileTableHandle& fileTable = *copy;
  EXPECT_EQ(copy->serialize(), handle->serialize());
  EXPECT_EQ(copy->toString(), handle->toString());
  EXPECT_EQ(copy->connectorId(), "paimon");
  EXPECT_EQ(fileTable.name(), "orders");
  EXPECT_EQ(copy->tablePath(), "/warehouse/orders");
  EXPECT_EQ(copy->tableType(), PaimonTableType::kAppendOnly);
  EXPECT_EQ(copy->targetSchemaId(), 20);
  EXPECT_EQ(copy->readMode(), ReadMode::kSnapshot);
  EXPECT_EQ(copy->partitionKeys(), (std::vector<int32_t>{11}));
  EXPECT_TRUE(copy->primaryKeys().empty());
  EXPECT_EQ(fileTable.sampleRate(), 0.25);
  EXPECT_EQ(fileTable.dbName(), "test_db");
  EXPECT_FALSE(fileTable.supportsIndexLookup());
  EXPECT_FALSE(fileTable.dataColumns()->containsChild("p"));
  EXPECT_EQ(fileTable.dataColumns()->size(), 4);
  EXPECT_EQ(fileTable.tableParameters().at("nested-field"), "true");
  EXPECT_EQ(copy->targetSchema()->options().at("nested-field"), "false");
  EXPECT_EQ(copy->readOptions().size(), 1);
  EXPECT_EQ(
      filterColumnNames(fileTable),
      (std::set<std::string>{"p", "payload", "aux"}));
  EXPECT_EQ(*fileTable.remainingFilter(), *remaining);
  EXPECT_TRUE(
      fileTable.subfieldFilters().at(Subfield("payload.a"))->testNull());
  EXPECT_EQ(handle->remainingFilter(), remaining);
}

TEST_F(PaimonHandleTest, vectorConstantExpressionRoundTrip) {
  auto constant = std::make_shared<core::ConstantTypedExpr>(
      makeArrayVector<int64_t>({{1, 5, 9}}));
  auto expr = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(),
      "contains",
      constant,
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "aux"));
  const auto handle = table({}, expr);
  const auto copy = ISerializable::deserialize<PaimonTableHandle>(
      handle->serialize(), pool());
  EXPECT_EQ(*copy->remainingFilter(), *expr);
  EXPECT_EQ(filterColumnNames(*copy), (std::set<std::string>{"aux"}));
}

TEST_F(PaimonHandleTest, historyAndKeyOrder) {
  const auto historical = std::make_shared<PaimonSchema>(
      1,
      1,
      ROW({{"old_id", INTEGER()}, {"p", VARCHAR()}}),
      PaimonTypeMetadata{
          "ROW",
          false,
          std::nullopt,
          {},
          {{"INTEGER", false, 7}, {"VARCHAR", true, 11, {20}}}},
      std::vector<int32_t>{11},
      std::vector<int32_t>{11, 7});
  const auto target = std::make_shared<PaimonSchema>(
      20,
      3,
      ROW({{"p", VARCHAR()}, {"id", BIGINT()}, {"old_id", VARCHAR()}}),
      PaimonTypeMetadata{
          "ROW",
          false,
          std::nullopt,
          {},
          {{"VARCHAR", true, 11, {20}},
           {"BIGINT", false, 7},
           {"VARCHAR", true, 12, {50}}}},
      std::vector<int32_t>{11},
      std::vector<int32_t>{11, 7},
      std::unordered_map<std::string, std::string>{{"file.format", "nimble"}});
  const auto handle = std::make_shared<PaimonTableHandle>(
      "paimon",
      "orders",
      "/warehouse/orders",
      PaimonTableType::kPrimaryKey,
      20,
      std::vector<PaimonSchemaPtr>{target, historical});
  const auto copy =
      ISerializable::deserialize<PaimonTableHandle>(handle->serialize());
  EXPECT_EQ(copy->serialize(), handle->serialize());
  EXPECT_EQ(copy->primaryKeys(), (std::vector<int32_t>{11, 7}));
  EXPECT_EQ(copy->schema(1)->rowType()->nameOf(0), "old_id");
  EXPECT_EQ(*copy->schema(1)->fieldType(7), *INTEGER());
  EXPECT_EQ(*copy->targetSchema()->fieldType(7), *BIGINT());
  EXPECT_EQ(
      copy->schema(1)->fieldIndexPath(7), (std::vector<column_index_t>{0}));
  EXPECT_EQ(
      copy->targetSchema()->fieldIndexPath(7),
      (std::vector<column_index_t>{1}));
  EXPECT_EQ(
      copy->targetSchema()->fieldIdPath(Subfield("old_id")),
      (std::vector<int32_t>{12}));
  EXPECT_EQ(copy->schema(1)->options().at("file.format"), "orc");
  EXPECT_EQ(copy->schema(1)->options().at("bucket"), "1");
  EXPECT_EQ(copy->tableParameters().at("file.format"), "nimble");
  EXPECT_EQ(copy->tableParameters().count("bucket"), 0);
  EXPECT_TRUE(copy->readOptions().empty());
  VELOX_ASSERT_THROW(copy->schema(19), "Missing Paimon schema 19");
  VELOX_ASSERT_THROW(copy->schema(1)->fieldType(12), "Unknown Paimon field ID");
}

TEST_F(PaimonHandleTest, tableIdentityValidation) {
  const auto good = table()->serialize();
  auto obj = good;
  obj["schemas"] = folly::dynamic::array;
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()), "Missing Paimon schema 20");
  obj = good;
  obj["targetSchemaId"] = 21;
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()), "Missing Paimon schema 21");
  obj["targetSchemaId"] = -1;
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()), "must be non-negative");
  obj = good;
  obj["schemas"].push_back(good["schemas"][0]);
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()), "Duplicate Paimon schema ID");
  obj = good;
  obj["tableType"] = "PRIMARY_KEY";
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()),
      "table type disagrees with primary keys");
  for (const auto* key : {"connectorId", "tableName", "tablePath"}) {
    obj = good;
    obj[key] = "";
    VELOX_ASSERT_THROW(
        PaimonTableHandle::create(obj, pool()), "must not be empty");
  }
  for (double rate :
       {0.0,
        -0.1,
        1.1,
        std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::quiet_NaN()}) {
    VELOX_ASSERT_THROW(
        table({}, nullptr, {}, {}, rate), "sample rate must be in (0, 1]");
  }
}

TEST_F(PaimonHandleTest, serdeVersionAndRequiredFields) {
  for (const auto& good :
       {table()->serialize(), table()->columnHandle(7)->serialize()}) {
    for (const auto& version :
         {folly::dynamic(0),
          folly::dynamic(2),
          folly::dynamic(int64_t{1} << 32),
          folly::dynamic("1")}) {
      auto obj = good;
      obj["version"] = version;
      EXPECT_THROW(
          ISerializable::deserialize<ISerializable>(obj, pool()),
          std::exception);
    }
    for (const auto& key : good.keys()) {
      if (key.asString() == "name") {
        continue;
      }
      auto obj = good;
      obj.erase(key);
      EXPECT_THROW(
          ISerializable::deserialize<ISerializable>(obj, pool()),
          std::exception)
          << key;
    }
  }
  for (auto mode :
       {ReadMode::kSnapshot, ReadMode::kIncremental, ReadMode::kStreaming}) {
    auto obj = table()->serialize();
    obj["readMode"] = PaimonTableHandle::readModeName(mode);
    const auto copy = ISerializable::deserialize<PaimonTableHandle>(obj);
    EXPECT_EQ(copy->readMode(), mode);
    EXPECT_EQ(copy->serialize(), obj);
  }
  auto obj = table()->serialize();
  obj["readMode"] = "UNKNOWN";
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()), "Unsupported Paimon read mode");
}

TEST_F(PaimonHandleTest, assignmentIdentityAndAliases) {
  const auto handle = table();
  const auto id = handle->columnHandle(7);
  handle->validateAssignments(
      ROW({{"output_id", BIGINT()}}), {{"output_id", id}});
  handle->validateAssignments(ROW({}, {}), {});
  EXPECT_TRUE(id->requiredSubfields().empty());
  VELOX_ASSERT_THROW(
      handle->validateAssignments(ROW({{"x", BIGINT()}}), {}),
      "Missing Paimon assignment");
  VELOX_ASSERT_THROW(
      handle->validateAssignments(ROW({{"x", INTEGER()}}), {{"x", id}}),
      "output type does not match");
  VELOX_ASSERT_THROW(
      handle->validateAssignments(
          ROW({{"x", BIGINT()}, {"y", BIGINT()}}), {{"x", id}, {"y", id}}),
      "require a Project");
  VELOX_ASSERT_THROW(handle->columnHandle(9), "require a top-level field ID");
  VELOX_ASSERT_THROW(
      handle->validateAssignments(ROW({{"x", BIGINT()}}), {{"x", nullptr}}),
      "require PaimonColumnHandle");

  const auto good = id->serialize();
  auto check = [&](const folly::dynamic& obj, const std::string& error) {
    const auto column = ISerializable::deserialize<PaimonColumnHandle>(obj);
    VELOX_ASSERT_THROW(handle->validateColumn(*column), error);
  };
  auto obj = good;
  obj["schemaId"] = 1;
  check(obj, "must use the target schema");
  obj = good;
  obj["columnName"] = "old_id";
  check(obj, "name does not match its field ID");
  obj = good;
  obj["metadata"]["fieldId"] = 999;
  check(obj, "Unknown Paimon field ID");
  obj = good;
  obj["metadata"]["nullable"] = true;
  check(obj, "metadata does not match");
  obj = good;
  obj["columnType"] =
      FileColumnHandle::columnTypeName(ColumnType::kPartitionKey);
  check(obj, "role does not match partition keys");
  obj = good;
  obj["dataType"] = INTEGER()->serialize();
  obj["metadata"]["logicalType"] = "INTEGER";
  check(obj, "type does not match the target schema");
  obj = handle->columnHandle(8)->serialize();
  obj["metadata"]["children"][0]["nullable"] = false;
  check(obj, "metadata does not match");
}

TEST_F(PaimonHandleTest, malformedHandleWire) {
  const auto good = table()->serialize();
  auto obj = good;
  obj["readOptions"]["nested-field"] = true;
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()), "read options must be strings");
  obj = good;
  obj["sampleRate"] = "1";
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()),
      "Expected Paimon sample rate number");
  obj = good;
  obj["targetSchemaId"] = "20";
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()),
      "Expected Paimon target schema ID");
  obj = good;
  obj["tableType"] = "UNKNOWN";
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()), "Unknown PaimonTableType");
  obj = good;
  const folly::dynamic filter =
      folly::dynamic::object("subfield", serializeSubfield(Subfield("id")))(
          "filter", common::IsNotNull().serialize());
  obj["subfieldFilters"] = folly::dynamic::array(filter, filter);
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()),
      "Duplicate Paimon subfield filter");
  obj = good;
  obj["filterColumnHandles"] = folly::dynamic::array(good);
  VELOX_ASSERT_THROW(
      PaimonTableHandle::create(obj, pool()),
      "Missing Paimon filter column handle");
}

TEST_F(PaimonHandleTest, subfieldIdentitySurvivesSerde) {
  const auto schema = std::make_shared<PaimonSchema>(
      1,
      3,
      ROW(
          {{"a.b", BIGINT()},
           {"a", ROW({{"b", BIGINT()}})},
           {"m", MAP(VARCHAR(), BIGINT())}}),
      PaimonTypeMetadata{
          "ROW",
          false,
          std::nullopt,
          {},
          {{"BIGINT", true, 1},
           {"ROW", true, 2, {}, {{"BIGINT", true, 3}}},
           {"MAP",
            true,
            4,
            {},
            {{"VARCHAR", false, std::nullopt, {100}}, {"BIGINT", true}}}}});
  const auto literal = Subfield::create("a.b");
  const Subfield nested("a.b");
  ASSERT_EQ(literal->toString(), nested.toString());
  std::vector<std::unique_ptr<Subfield::PathElement>> elements;
  elements.push_back(std::make_unique<Subfield::NestedField>("m"));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("key\\\"[.]"));
  const Subfield mapKey(std::move(elements));
  common::SubfieldFilters filters;
  filters.emplace(
      literal->clone(), std::make_shared<common::BigintRange>(1, 1, false));
  filters.emplace(
      nested.clone(), std::make_shared<common::BigintRange>(2, 2, false));
  filters.emplace(
      mapKey.clone(), std::make_shared<common::BigintRange>(3, 3, false));
  const auto handle = std::make_shared<PaimonTableHandle>(
      "paimon",
      "special_names",
      "/warehouse/special_names",
      PaimonTableType::kAppendOnly,
      1,
      std::vector<PaimonSchemaPtr>{schema},
      std::move(filters));
  const auto copy =
      ISerializable::deserialize<PaimonTableHandle>(handle->serialize());
  EXPECT_EQ(copy->subfieldFilters().size(), 3);
  EXPECT_TRUE(copy->subfieldFilters().at(*literal)->testInt64(1));
  EXPECT_TRUE(copy->subfieldFilters().at(nested)->testInt64(2));
  EXPECT_TRUE(copy->subfieldFilters().at(mapKey)->testInt64(3));
  EXPECT_EQ(copy->serialize(), handle->serialize());
  EXPECT_EQ(copy->toString(), handle->toString());
  std::vector<Subfield> required;
  required.push_back(mapKey.clone());
  const auto column = handle->columnHandle(4, std::move(required));
  const auto columnCopy =
      ISerializable::deserialize<PaimonColumnHandle>(column->serialize());
  ASSERT_EQ(columnCopy->requiredSubfields().size(), 1);
  EXPECT_EQ(columnCopy->requiredSubfields()[0], mapKey);
}

TEST_F(PaimonHandleTest, predicateAndFilterColumnValidation) {
  auto missing =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "missing");
  VELOX_ASSERT_THROW(table({}, missing), "Unknown Paimon filter field");
  auto wrongType =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "id");
  VELOX_ASSERT_THROW(
      table({}, wrongType), "expression field type does not match");
  VELOX_ASSERT_THROW(
      table({}, std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "id")),
      "remaining filter must return BOOLEAN");
  common::SubfieldFilters invalidPath;
  invalidPath.emplace(
      Subfield("payload.missing"), std::make_shared<common::IsNull>());
  VELOX_ASSERT_THROW(table(std::move(invalidPath)), "Unknown Paimon subfield");
  common::SubfieldFilters missingFilter;
  missingFilter.emplace(Subfield("id"), nullptr);
  VELOX_ASSERT_THROW(
      table(std::move(missingFilter)), "Missing Paimon subfield filter");
  const auto p = table()->columnHandle(11);
  VELOX_ASSERT_THROW(
      table({}, nullptr, {p, p}), "Duplicate Paimon filter column");
  auto obj = p->serialize();
  obj["columnType"] = FileColumnHandle::columnTypeName(ColumnType::kRegular);
  const auto wrongRole = ISerializable::deserialize<PaimonColumnHandle>(obj);
  VELOX_ASSERT_THROW(
      table({}, nullptr, {wrongRole}), "role does not match partition keys");
}

TEST_F(PaimonHandleTest, lambdaScopeAndCaptures) {
  const auto input = targetSchema()->rowType();
  auto expr = parseExpr("all_match(arr, p -> p > aux)", input);
  auto handle = table({}, expr);
  // 'p' shadows the table's partition column of a different type.
  EXPECT_EQ(filterColumnNames(*handle), (std::set<std::string>{"arr", "aux"}));
  expr = parseExpr("all_match(arr, x -> x > aux AND p = 'east')", input);
  handle = table({}, expr);
  EXPECT_EQ(
      filterColumnNames(*handle), (std::set<std::string>{"arr", "aux", "p"}));
  const auto copy = ISerializable::deserialize<PaimonTableHandle>(
      handle->serialize(), pool());
  EXPECT_EQ(filterColumnNames(*copy), filterColumnNames(*handle));
  EXPECT_EQ(*copy->remainingFilter(), *expr);

  const auto badCapture = std::make_shared<core::LambdaTypedExpr>(
      ROW({{"p", BIGINT()}}),
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "missing"));
  const auto call = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(),
      "all_match",
      std::make_shared<core::FieldAccessTypedExpr>(ARRAY(BIGINT()), "arr"),
      badCapture);
  VELOX_ASSERT_THROW(table({}, call), "Unknown Paimon filter field: missing");
}

TEST_F(PaimonHandleTest, fileDataSourceConsumesScanRequirements) {
  checkFilePreparation(false);
}

TEST_F(PaimonHandleTest, fileDataSourceConsumesFilterOnlyRequirements) {
  checkFilePreparation(true);
}

TEST_F(PaimonHandleTest, emptyRequiredSubfieldsReadsWholeColumn) {
  checkFilePreparation(false, false);
}

} // namespace
} // namespace facebook::velox::connector::hive::paimon
