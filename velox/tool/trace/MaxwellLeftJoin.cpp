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
#include <spark/connect/base.pb.h>

#include <algorithm>
#include <utility>

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
#include "velox/tool/trace/OperatorReplayerBase.h"
#include "velox/tool/trace/TraceReplayTaskRunner.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::parquet;
using namespace facebook::velox::test;

DEFINE_bool(
    base_in_left,
    false,
    "Base file in the left side of the join or scan base file");
DEFINE_string(base_file_path, "", "Base file path");
DEFINE_string(seq_file_path, "", "Seq file path");
DEFINE_string(operator_type, "join", "scan or join");
DEFINE_bool(copy_results, true, "copy result");

namespace {
const auto ioExecutor{std::make_unique<folly::IOThreadPoolExecutor>(
    std::thread::hardware_concurrency() * 2.0,
    std::make_shared<folly::NamedThreadFactory>("MaxwellIOExecutors"))};
void initialize() {
  // Default memory allocator used throughout this example.
  VELOX_CHECK(!FLAGS_base_file_path.empty());
  VELOX_CHECK(!FLAGS_seq_file_path.empty());
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
            ioExecutor.get());
    connector::registerConnector(hiveConnector);
  }

  parquet::registerParquetReaderFactory();
  filesystems::registerLocalFileSystem();
}

class MaxwellBatchJoinBenchmark {
 public:
  explicit MaxwellBatchJoinBenchmark(
      std::string baseFileName,
      std::string seqFileName,
      const size_t numThreads,
      memory::MemoryPool* pool)
      : baseFileName_(std::move(baseFileName)),
        seqFileName_(std::move(seqFileName)),
        cpuExecutor_{std::make_unique<folly::CPUThreadPoolExecutor>(
            numThreads,
            std::make_shared<folly::NamedThreadFactory>(
                "MaxwellCPUExecutors"))},
        pool_(pool) {}

  void runJoin(bool baseInLeft) const {
    const auto baseScanType =
        makeScanType(baseFileName_, {0, 1, 2, 3, 4, 5}, pool_);
    const auto seqScanType =
        makeScanType(seqFileName_, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, pool_);
    const auto baseProjects = makeProjects(
        baseScanType->names(), "base_", [](std::vector<std::string>& projects) {
          projects.emplace_back(
              "split_part(_hoodie_record_key, ':', 1) AS l_uid");
        });
    const auto seqProjects = makeProjects(seqScanType->names(), "seq_");
    auto planWithSplits = createMergeJoin(
        baseScanType,
        seqScanType,
        baseFileName_,
        seqFileName_,
        baseProjects,
        seqProjects,
        makeOutputColumns(
            baseScanType->names(), "base_", seqScanType->names(), "seq_"),
        baseInLeft);

    tool::trace::TraceReplayTaskRunner smjRunner(
        planWithSplits.plan, createQueryCtx("maxwell_smj"));
    smjRunner.maxDrivers(8);
    for (const auto& [nodeId, splits] : planWithSplits.splits) {
      smjRunner.splits(nodeId, splits);
    }
    auto [task, result] = smjRunner.run(FLAGS_copy_results);
    LOG(ERROR) << result->childAt(1)->toString();
    printStats(planWithSplits.plan->id(), task);
    printStats(planWithSplits.baseProjectID, task);
    printStats(planWithSplits.baseScanID, task);
    printStats(planWithSplits.seqProjectID, task);
    printStats(planWithSplits.seqScanID, task);
  }

  void runScan(bool baseInLeft) const {
    const auto baseScanType =
        makeScanType(baseFileName_, {0, 1, 2, 3, 4, 5}, pool_);
    const auto seqScanType =
        makeScanType(seqFileName_, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, pool_);
    std::unique_ptr<PlanWithSplits> planWithSplits;
    if (baseInLeft) {
      planWithSplits = std::make_unique<PlanWithSplits>(
          createTableScan(baseScanType, FLAGS_base_file_path));
    } else {
      planWithSplits = std::make_unique<PlanWithSplits>(
          createTableScan(seqScanType, FLAGS_seq_file_path));
    }

    tool::trace::TraceReplayTaskRunner smjRunner(
        planWithSplits->plan, createQueryCtx("maxwell_smj"));
    smjRunner.maxDrivers(8);
    for (const auto& [nodeId, splits] : planWithSplits->splits) {
      smjRunner.splits(nodeId, splits);
    }
    auto [task, result] = smjRunner.run(FLAGS_copy_results);
    LOG(ERROR) << result->toString();
    printStats(planWithSplits->plan->id(), task);
  }

 private:
  std::shared_ptr<core::QueryCtx> createQueryCtx(
      const std::string& queryId) const {
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

  static std::unique_ptr<facebook::velox::parquet::ParquetReader> createReader(
      const std::string& path,
      const dwio::common::ReaderOptions& opts) {
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<LocalReadFile>(path), opts.memoryPool());
    return std::make_unique<facebook::velox::parquet::ParquetReader>(
        std::move(input), opts);
  }

  static RowTypePtr parquetSchema(
      const std::string& fileName,
      memory::MemoryPool* pool) {
    dwio::common::ReaderOptions readerOptions{pool};
    const auto reader = createReader(fileName, readerOptions);
    return reader->rowType();
  }

  static RowTypePtr makeScanType(
      const std::string& fileName,
      const std::vector<size_t>& indices,
      memory::MemoryPool* pool) {
    const auto& schema = parquetSchema(fileName, pool);
    const auto& names = schema->names();
    const auto& types = schema->children();
    std::vector<std::string> scanNames;
    scanNames.reserve(indices.size());
    std::vector<std::shared_ptr<const Type>> scanTypes;
    scanTypes.reserve(indices.size());
    for (const auto i : indices) {
      scanNames.emplace_back(names[i]);
      scanTypes.emplace_back(types[i]);
    }
    return ROW(std::move(scanNames), std::move(scanTypes));
  }

  static std::vector<std::string> makeProjects(
      const std::vector<std::string>& names,
      const std::string& prefix,
      const std::function<void(std::vector<std::string>&)>& postProcess =
          [](std::vector<std::string>&) {}) {
    std::vector<std::string> projects;
    projects.reserve(names.size() + 1);
    for (const auto& name : names) {
      projects.emplace_back(fmt::format("{} AS {}{}", name, prefix, name));
    }
    postProcess(projects);
    return projects;
  }

  static std::vector<std::string> makeOutputColumns(
      const std::vector<std::string>& names1,
      const std::string& prefix1,
      const std::vector<std::string>& names2,
      const std::string& prefix2) {
    std::vector<std::string> outputColumns;
    outputColumns.reserve(names1.size() + names2.size());
    for (const auto& name : names1) {
      outputColumns.emplace_back(fmt::format("{}{}", prefix1, name));
    }
    for (const auto& name : names2) {
      outputColumns.emplace_back(fmt::format("{}{}", prefix2, name));
    }
    return outputColumns;
  }

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    core::PlanNodeId baseScanID;
    core::PlanNodeId seqScanID;
    core::PlanNodeId baseProjectID;
    core::PlanNodeId seqProjectID;
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits;

    explicit PlanWithSplits(
        core::PlanNodePtr _plan,
        core::PlanNodeId _baseScanID = "",
        core::PlanNodeId _seqScanID = "",
        core::PlanNodeId _baseProjectID = "",
        core::PlanNodeId _seqProjectID = "",
        const std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>&
            _splits = {})
        : plan(std::move(_plan)),
          baseScanID(std::move(_baseScanID)),
          seqScanID(std::move(_seqScanID)),
          baseProjectID(std::move(_baseProjectID)),
          seqProjectID(std::move(_seqProjectID)),
          splits(_splits) {}
  };

  static exec::Split makeSplit(const std::string& filePath) {
    return exec::Split(std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId,
        "file:" + filePath,
        dwio::common::FileFormat::PARQUET));
  }

  static PlanWithSplits createMergeJoin(
      const RowTypePtr& baseScanType,
      const RowTypePtr& seqScanType,
      const std::string& baseFilePath,
      const std::string& seqFilePath,
      const std::vector<std::string>& baseProjects,
      const std::vector<std::string>& seqProjects,
      const std::vector<std::string>& outputColumns,
      bool baseInLeft) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId baseScanId;
    core::PlanNodeId seqScanId;
    core::PlanNodeId baseProjectId;
    core::PlanNodeId seqProjectId;
    core::PlanNodePtr plan;
    if (baseInLeft) {
      LOG(ERROR) << "Left Join";
      plan = PlanBuilder(planNodeIdGenerator)
                 .tableScan(baseScanType)
                 .capturePlanNodeId(baseScanId)
                 .project(baseProjects)
                 .capturePlanNodeId(baseProjectId)
                 .mergeJoin(
                     {"l_uid"},
                     {"seq__hoodie_record_key"},
                     PlanBuilder(planNodeIdGenerator)
                         .tableScan(seqScanType)
                         .capturePlanNodeId(seqScanId)
                         .project(seqProjects)
                         .capturePlanNodeId(seqProjectId)
                         .planNode(),
                     "",
                     outputColumns,
                     core::JoinType::kLeft)
                 .planNode();

    } else {
      LOG(ERROR) << "Right Join";
      plan = PlanBuilder(planNodeIdGenerator)
                 .tableScan(seqScanType)
                 .capturePlanNodeId(seqScanId)
                 .project(seqProjects)
                 .capturePlanNodeId(seqProjectId)
                 .mergeJoin(
                     {"seq__hoodie_record_key"},
                     {"l_uid"},
                     PlanBuilder(planNodeIdGenerator)
                         .tableScan(baseScanType)
                         .capturePlanNodeId(baseScanId)
                         .project(baseProjects)
                         .capturePlanNodeId(baseProjectId)
                         .planNode(),
                     "",
                     outputColumns,
                     core::JoinType::kRight)
                 .planNode();
    }

    return PlanWithSplits{
        plan,
        baseScanId,
        seqScanId,
        baseProjectId,
        seqProjectId,
        {{baseScanId, {makeSplit(baseFilePath)}},
         {seqScanId, {makeSplit(seqFilePath)}}}};
  }

  static PlanWithSplits createTableScan(
      const RowTypePtr& scanType,
      const std::string& filePath) {
    const auto planNodeIdGenerator =
        std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId scanNodId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .tableScan(scanType)
                    .capturePlanNodeId(scanNodId)
                    .planNode();
    return PlanWithSplits{
        std::move(plan),
        scanNodId,
        scanNodId,
        scanNodId,
        scanNodId,
        {{scanNodId, {makeSplit(filePath)}}}};
  }

  static void printStats(
      const core::PlanNodeId& planNodeId,
      const std::shared_ptr<exec::Task>& task) {
    const auto planStats = exec::toPlanStats(task->taskStats());
    const auto& stats = planStats.at(planNodeId);
    for (const auto& [name, operatorStats] : stats.operatorStats) {
      LOG(INFO) << "Stats of node " << planNodeId << " operator " << name
                << " : " << operatorStats->toString();
    }
    // LOG(INFO) << "Memory usage: " << task->pool()->treeMemoryUsage(false);
  }

  const std::string baseFileName_;
  const std::string seqFileName_;
  const std::unique_ptr<folly::CPUThreadPoolExecutor> cpuExecutor_;
  memory::MemoryPool* const pool_;
};
} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  initialize();
  const auto pool = memory::memoryManager()->addLeafPool();
  const auto benchmark = std::make_unique<MaxwellBatchJoinBenchmark>(
      FLAGS_base_file_path, FLAGS_seq_file_path, 4, pool.get());
  if (FLAGS_operator_type == "join") {
    benchmark->runJoin(FLAGS_base_in_left);
  } else {
    benchmark->runScan(FLAGS_base_in_left);
  }
  return 0;
}
