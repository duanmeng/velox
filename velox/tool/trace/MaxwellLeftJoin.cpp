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

#include <exec/PlanNodeStats.h>
#include <folly/init/Init.h>
#include <algorithm>
#include <utility>

#include "TraceReplayTaskRunner.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/Task.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/tool/trace/OperatorReplayerBase.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::parquet;
using namespace facebook::velox::test;

namespace {
const auto cpuExecutor_{std::make_unique<folly::CPUThreadPoolExecutor>(
    std::thread::hardware_concurrency() * 2.0,
    std::make_shared<folly::NamedThreadFactory>("MaxwellCPUExecutors"))};
const auto ioExecutor_{std::make_unique<folly::IOThreadPoolExecutor>(
    std::thread::hardware_concurrency() * 2.0,
    std::make_shared<folly::NamedThreadFactory>("MaxwellIOExecutors"))};

std::shared_ptr<core::QueryCtx> createQueryCtx(const std::string& queryId) {
  auto queryPool =
      memory::memoryManager()->addRootPool(queryId, memory::kMaxMemory);
  return core::QueryCtx::create(
      cpuExecutor_.get(),
      core::QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      nullptr,
      std::move(queryPool),
      cpuExecutor_.get());
}

void init() {
  // Default memory allocator used throughout this example.
  memory::MemoryManager::initialize({});

  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  if (!facebook::velox::connector::hasConnectorFactory("hive")) {
    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
    const auto hiveConnector =
        connector::getConnectorFactory("hive")->newConnector(
            "test-hive",
            std::make_shared<config::ConfigBase>(
                std::unordered_map<std::string, std::string>()),
            ioExecutor_.get());
    connector::registerConnector(hiveConnector);
  }

  parquet::registerParquetReaderFactory();
  filesystems::registerLocalFileSystem();
}

std::vector<std::string> leftKeys{
    "_hoodie_record_key",
};
std::vector<std::string> rightKeys{
    "_hoodie_record_key",
    "backfill_live_ecom_product_seq_uid_filtered_250113_new_product_app_id",
};
RowTypePtr leftScanType() {
  return ROW(
      {
          "_hoodie_record_key",
      },
      {
          VARCHAR(),
      });
}

RowTypePtr rightScanType() {
  return ROW(
      {
          "_hoodie_record_key",
          "backfill_live_ecom_product_seq_uid_filtered_250113_new_product_app_id",
      },
      {
          VARCHAR(),
          ROW({"c0"},
              {ROW(
                  {
                      "value",
                  },
                  {
                      ARRAY(BIGINT()),
                  })}),
      });
}

struct PlanWithSplits {
  core::PlanNodePtr plan;
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits;

  explicit PlanWithSplits(
      core::PlanNodePtr _plan,
      core::PlanNodeId _leftScanId = "",
      core::PlanNodeId _rightScanId = "",
      const std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>&
          _splits = {})
      : plan(std::move(_plan)),
        leftScanId(std::move(_leftScanId)),
        rightScanId(std::move(_rightScanId)),
        splits(_splits) {}
};

RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
  std::vector<std::string> names = a->names();
  std::vector<TypePtr> types = a->children();

  for (auto i = 0; i < b->size(); ++i) {
    names.push_back(b->nameOf(i));
    types.push_back(b->childAt(i));
  }

  return ROW(std::move(names), std::move(types));
}

exec::Split makeSplit(const std::string& filePath) {
  return exec::Split(std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, "file:" + filePath, dwio::common::FileFormat::PARQUET));
}

PlanWithSplits createTableScan(
    const RowTypePtr& scanType,
    const std::string& filePath) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodId;
  core::PlanNodeId rightScanId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(scanType)
                  .capturePlanNodeId(scanNodId)
                  .planNode();
  return PlanWithSplits{
      plan,
      scanNodId,
      rightScanId,
      {{scanNodId, {makeSplit(filePath)}},
       {rightScanId, {makeSplit(filePath)}}}};
}

PlanWithSplits createMergeJoin(
    const RowTypePtr& leftScanType,
    const RowTypePtr& rightScanType,
    const std::string& leftFilePath,
    const std::string& rightFilePath) {
  const auto outputColumns = concat(leftScanType, rightScanType)->names();
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(leftScanType)
          .capturePlanNodeId(leftScanId)
          .project({"split_part(_hoodie_record_key, ':', 1) AS l_uid"})
          .mergeJoin(
              {"l_uid"},
              {"_hoodie_record_key"},
              PlanBuilder(planNodeIdGenerator)
                  .tableScan(rightScanType)
                  .capturePlanNodeId(rightScanId)
                  .planNode(),
              "",
              {"l_uid",
               "backfill_live_ecom_product_seq_uid_filtered_250113_new_product_app_id"},
              core::JoinType::kLeft)
          .planNode();

  return PlanWithSplits{
      plan,
      leftScanId,
      rightScanId,
      {{leftScanId, {makeSplit(leftFilePath)}},
       {rightScanId, {makeSplit(rightFilePath)}}}};
}

void printStats(
    const core::PlanNodeId& planNodeId,
    const std::shared_ptr<exec::Task>& task) {
  const auto planStats = exec::toPlanStats(task->taskStats());
  const auto& stats = planStats.at(planNodeId);
  for (const auto& [name, operatorStats] : stats.operatorStats) {
    LOG(INFO) << "Stats of smj " << name << " : " << operatorStats->toString();
  }
  //LOG(INFO) << "Memory usage: " << task->pool()->treeMemoryUsage(false);
}

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  init();
  auto pool = memory::memoryManager()->addLeafPool();
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));
  const std::string leftFilePath =
      "/Users/bytedance/Downloads/"
      "00001023-0_1023-1-17111#20250104165820#20250104173115_20250104173115.parquet";
  const std::string rightFilePath =
      "/Users/bytedance/Downloads/"
      "00001023-0_630-3-23689#20250114210208#20250115002758_20250115002758.parquet";

  auto planWithSplits = createMergeJoin(
      leftScanType(), rightScanType(), leftFilePath, rightFilePath);

  tool::trace::TraceReplayTaskRunner smjRunner(
      planWithSplits.plan, createQueryCtx("maxwell_smj"));
  for (const auto& [nodeId, splits] : planWithSplits.splits) {
    smjRunner.splits(nodeId, splits);
  }
  auto [task, result] = smjRunner.run(true);
  printStats(planWithSplits.plan->id(), task);
  printStats(planWithSplits.leftScanId, task);
  printStats(planWithSplits.rightScanId, task);


  LOG(ERROR) << "size = " << result->size();
  LOG(ERROR) << result->childAt(1)->toString();
  LOG(ERROR) << result->childAt(1)->encoding();

  return 0;
}
