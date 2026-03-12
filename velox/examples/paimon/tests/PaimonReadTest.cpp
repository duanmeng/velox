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

#include <gtest/gtest.h>
#include <iostream>

#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/examples/paimon/PaimonConnector.h"
#include "velox/examples/paimon/PaimonConnectorSplit.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::paimon;

class PaimonReadTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    parquet::registerParquetReaderFactory();

    // Register Paimon connector
    auto factory = std::make_shared<PaimonConnectorFactory>();
    auto config = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>());
    auto connector = factory->newConnector("test-paimon", config);
    connector::registerConnector(connector);
  }

  void TearDown() override {
    connector::unregisterConnector("test-paimon");
    HiveConnectorTestBase::TearDown();
  }

  std::string getTestFilePath(const std::string& fileName) {
    // Try multiple possible paths
    std::vector<std::string> paths = {
        fileName,
        "velox/examples/paimon/tests/" + fileName,
        "../../../../velox/examples/paimon/tests/" +
            fileName // From build dir to source dir
    };

    for (const auto& path : paths) {
      if (fs::exists(path)) {
        return fs::absolute(path).string();
      }
    }

    // Fallback to getDataFilePath which uses current_path + fileName
    return facebook::velox::test::getDataFilePath("", fileName);
  }
};

TEST_F(PaimonReadTest, readBasic) {
  // 1. Data file path
  std::string nationFile = getTestFilePath("nation.parquet");

  // 2. Construct PaimonConnectorSplit with 3 data files
  std::vector<PaimonDataFileMeta> files;
  for (int i = 0; i < 3; ++i) {
    // We can just pass max for length if we don't know the file size.
    // The reader should handle it correctly by reading the footer.
    files.push_back({nationFile, std::numeric_limits<uint64_t>::max(), 0});
  }

  // 2.1 Baseline: Hive Connector reading nation.parquet twice (as 3 splits)
  // This simulates reading the same data multiple times
  std::vector<std::shared_ptr<connector::ConnectorSplit>> hiveSplits;
  for (int i = 0; i < 3; ++i) {
    hiveSplits.push_back(
        std::make_shared<hive::HiveConnectorSplit>(
            "test-hive", nationFile, dwio::common::FileFormat::PARQUET));
  }

  // 3. Construct TableScan plan
  auto rowType = ROW(
      {{"nationkey", BIGINT()},
       {"name", VARCHAR()},
       {"regionkey", BIGINT()},
       {"comment", VARCHAR()}});

  // 3.1 Run baseline
  auto baselinePlan = PlanBuilder().tableScan(rowType).planNode();
  auto baselineResult = AssertQueryBuilder(baselinePlan)
                            .splits(hiveSplits)
                            .copyResultBatches(pool());

  auto split = std::make_shared<PaimonConnectorSplit>(
      "test-paimon",
      std::move(files),
      std::unordered_map<std::string, std::optional<std::string>>{},
      0);

  auto tableHandle = std::make_shared<hive::HiveTableHandle>(
      "test-paimon",
      "nation",
      common::SubfieldFilters{},
      nullptr, // remainingFilter
      rowType // dataColumns
  );

  connector::ColumnHandleMap assignments;
  for (int i = 0; i < rowType->size(); ++i) {
    auto name = rowType->nameOf(i);
    auto type = rowType->childAt(i);
    assignments[name] = std::make_shared<hive::HiveColumnHandle>(
        name, hive::HiveColumnHandle::ColumnType::kRegular, type, type);
  }

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(rowType)
                  .tableHandle(tableHandle)
                  .assignments(assignments)
                  .endTableScan()
                  .planNode();

  // 4. Execute and copy results
  auto results =
      AssertQueryBuilder(plan).split(split).copyResultBatches(pool());

  // 5. Verify results
  assertEqualResults(baselineResult, results);
}
