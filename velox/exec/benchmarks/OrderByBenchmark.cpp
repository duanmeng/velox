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
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_int32(num_payload_groups, 32, "num of payload groups in each vector");
DEFINE_int32(num_input_vectors, 1000, "num of input vectors");
DEFINE_int32(num_rows_per_vector, 1024, "num of rows per vector");
DEFINE_bool(sort_inputs, false, "sort input vectors");
DEFINE_bool(columnar, true, "Use columnar sort");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
struct TestCase {
  // Dataset to be processed by the below plans.
  std::vector<RowVectorPtr> rows;
  // OrderBy plan.
  std::shared_ptr<const core::PlanNode> plan;
};

class OrderByBenchmark : public VectorTestBase {
 public:
  std::vector<RowVectorPtr>
  makeRows(RowTypePtr type, int32_t numVectors, int32_t rowsPerVector) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(type, rowsPerVector, *pool_));
      for (auto i = 0; i < vector->childrenSize(); ++i) {
        BaseVector::flattenVector(vector->childAt(i));
        VELOX_CHECK(VectorEncoding::isFlat(vector->childAt(i)->encoding()));
      }
      vectors.push_back(vector);
    }
    return vectors;
  }

  std::vector<RowVectorPtr> makeOrderedVectors(
      int32_t numPayloadGrouops,
      int64_t numVectors,
      int32_t numPerVector) {
    const auto type = makeRowType(numPayloadGrouops);
    const auto rows = makeRows(type, numVectors, numPerVector);
    const auto plan = makeOrderByPlan({"c0", "c1", "c2"}, rows);
    const auto results =
        exec::test::AssertQueryBuilder(plan)
            .config(core::QueryConfig::kMaxOutputBatchRows, numPerVector)
            .config(core::QueryConfig::kPreferredOutputBatchRows, numPerVector)
            .copyResultBatches(pool_.get());
    return results;
  }

  static core::PlanNodePtr makeOrderByPlan(
      std::vector<std::string> keys,
      std::vector<RowVectorPtr> data) {
    exec::test::PlanBuilder builder;
    builder.values(std::move(data));
    builder.orderBy(keys, false);
    return builder.planNode();
  }

  static RowTypePtr makeRowType(int numPayloadGroups) {
    std::vector<TypePtr> types{BIGINT(), BIGINT(), BIGINT()};
    for (auto i = 0; i < numPayloadGroups; ++i) {
      types.push_back(VARCHAR());
      types.push_back(VARCHAR());
      types.push_back(VARCHAR());
      types.push_back(VARCHAR());
    }
    return {VectorMaker::rowType(std::move(types))};
  }

  void makeBenchmark(
      std::string name,
      int32_t numPayloadGrouops,
      int64_t numVectors,
      int32_t numPerVector) {
    auto test = std::make_unique<TestCase>();
    const auto type = makeRowType(numPayloadGrouops);
    if (FLAGS_sort_inputs) {
      test->rows =
        makeOrderedVectors(numPayloadGrouops, numVectors, numPerVector);
    } else {
      test->rows = makeRows(type, numVectors, numPerVector);
    }
    test->plan = makeOrderByPlan({"c0", "c1", "c2"}, test->rows);
    folly::addBenchmark(
        __FILE__,
        fmt::format(
            "{}_{}_{}_payload_groups", name, FLAGS_columnar, numPayloadGrouops),
        [plan = &test->plan, this]() {
          run(*plan, FLAGS_columnar);
          return 1;
        });
    cases_.push_back(std::move(test));
  }

  int64_t run(std::shared_ptr<const core::PlanNode> plan, bool nonMaterialized)
      const {
    const auto start = getCurrentTimeMicro();
    std::shared_ptr<Task> task;
    exec::test::AssertQueryBuilder(plan)
        .config(
            core::QueryConfig::kNonMaterializedSortBufferEnabled,
            nonMaterialized)
        .runWithoutResults(task);
    auto taskStats = toPlanStats(task->taskStats());
    const auto orderByNode = core::PlanNode::findFirstNode(
        plan.get(),
        [](const core::PlanNode* node) { return node->name() == "OrderBy"; });
    const auto orderByNodeId = orderByNode->id();
    auto& stats = taskStats.at(orderByNodeId);
    const auto addInputWallNanos = stats.addInputTiming.wallNanos;
    const auto addInputCpuNanos = stats.addInputTiming.cpuNanos;
    const auto finishWallNanos = stats.finishTiming.wallNanos;
    const auto finishCpuNanos = stats.finishTiming.cpuNanos;
    const auto sortTimeNanos = stats.finishTiming.wallNanos;
    const auto getOutputWallNanos = stats.getOutputTiming.wallNanos;
    const auto getOutputCpuNanos = stats.getOutputTiming.cpuNanos;
    const auto indexExtractionWallNanos =
        stats.customStats["IndexExtractionTime"].sum;
    std::cout << "Velox stats " << nonMaterialized << " Input "
              << succinctNanos(addInputWallNanos) << " "
              << " Output " << succinctNanos(getOutputWallNanos) << " "
              << " IndexExtraction " << succinctNanos(indexExtractionWallNanos)
              << " "
              << " Finish " << succinctNanos(finishWallNanos) << " "
              << std::endl;
    return getCurrentTimeMicro() - start;
  }

  std::vector<std::unique_ptr<TestCase>> cases_;
};
} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  OrderByBenchmark bm;
  bm.makeBenchmark("OrderBy", FLAGS_num_payload_groups, FLAGS_num_input_vectors, 1024);

  folly::runBenchmarks();
  return 0;
}
