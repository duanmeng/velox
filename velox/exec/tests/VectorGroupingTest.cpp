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

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "folly/experimental/EventCount.h"
#include "utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::test {

class VectorGroupingTest : public HiveConnectorTestBase {
 public:
  void testVectorGrouping(
      const std::vector<std::string>& groupKeys,
      const std::vector<RowVectorPtr>& input,
      const std::vector<RowVectorPtr>& excepteds,
      int32_t maxInputHolded) {
    CursorParameters params;
    params.planNode = PlanBuilder()
                          .values({input})
                          .vectorGrouping(groupKeys, maxInputHolded)
                          .planNode();
    params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
    auto cursor = TaskCursor::create(params);
    auto task = cursor->task();

    std::vector<RowVectorPtr> result;
    while (cursor->moveNext()) {
      result.push_back(cursor->current());
    }

    ASSERT_EQ(result.size(), excepteds.size());
    for (auto i = 0; i < excepteds.size(); i++) {
      ASSERT_EQ(excepteds[i]->size(), result[i]->size());
      for (auto j = 0; j < excepteds[i]->size(); j++) {
        ASSERT_EQ(
            excepteds[i]->childAt(0)->asFlatVector<int32_t>()->valueAt(j),
            result[i]->childAt(0)->asFlatVector<int32_t>()->valueAt(j));
      }
    }
    ASSERT_TRUE(
        waitForTaskStateChange(task.get(), TaskState::kFinished, 3'000'000));
  }
};

TEST_F(VectorGroupingTest, Sample) {
  auto input = std::vector<RowVectorPtr>{
      makeRowVector({makeFlatVector<int32_t>({1, 1, 2, 2})}),
      makeRowVector({makeFlatVector<int32_t>({2, 3, 3, 4})}),
      makeRowVector({makeFlatVector<int32_t>({4, 4, 5, 5})}),
      makeRowVector({makeFlatVector<int32_t>({5, 6, 6, 6})}),
  };
  auto excepted = std::vector<RowVectorPtr>{
      makeRowVector({makeFlatVector<int32_t>({1, 1})}),
      makeRowVector({makeFlatVector<int32_t>({2, 2, 2})}),
      makeRowVector({makeFlatVector<int32_t>({3, 3})}),
      makeRowVector({makeFlatVector<int32_t>({4, 4, 4})}),
      makeRowVector({makeFlatVector<int32_t>({5, 5, 5})}),
      makeRowVector({makeFlatVector<int32_t>({6, 6, 6})}),
  };

  testVectorGrouping({"c0"}, input, excepted, 2);
  testVectorGrouping({"c0"}, input, excepted, 3);
}

TEST_F(VectorGroupingTest, Sample2) {
  auto input = std::vector<RowVectorPtr>{
      makeRowVector({makeFlatVector<int32_t>({1, 1, 1, 2})}),
      makeRowVector({makeFlatVector<int32_t>({2, 2, 2, 2})}),
      makeRowVector({makeFlatVector<int32_t>({2, 3, 3, 3})}),
  };
  auto excepted = std::vector<RowVectorPtr>{
      makeRowVector({makeFlatVector<int32_t>({1, 1, 1})}),
      makeRowVector({makeFlatVector<int32_t>({2, 2, 2, 2, 2, 2})}),
      makeRowVector({makeFlatVector<int32_t>({3, 3, 3})}),
  };

  testVectorGrouping({"c0"}, input, excepted, 2);
  testVectorGrouping({"c0"}, input, excepted, 3);
}

TEST_F(VectorGroupingTest, Sample4) {
  auto input = std::vector<RowVectorPtr>{
      makeRowVector({makeFlatVector<int32_t>({1, 1, 2, 2})}),
      makeRowVector({makeFlatVector<int32_t>({2, 3, 3, 4})}),
      makeRowVector({makeFlatVector<int32_t>({4, 4, 5, 5})}),
      makeRowVector({makeFlatVector<int32_t>({5, 6, 6, 6})}),
      makeRowVector({makeFlatVector<int32_t>({7, 7, 8, 8})}),
      makeRowVector({makeFlatVector<int32_t>({9, 9, 10, 11})}),
      makeRowVector({makeFlatVector<int32_t>({12, 13, 14, 15})}),
      makeRowVector({makeFlatVector<int32_t>({16, 16, 16, 16})}),
  };
  auto excepted = std::vector<RowVectorPtr>{
      makeRowVector({makeFlatVector<int32_t>({1, 1})}),
      makeRowVector({makeFlatVector<int32_t>({2, 2, 2})}),
      makeRowVector({makeFlatVector<int32_t>({3, 3})}),
      makeRowVector({makeFlatVector<int32_t>({4, 4, 4})}),
      makeRowVector({makeFlatVector<int32_t>({5, 5, 5})}),
      makeRowVector({makeFlatVector<int32_t>({6, 6, 6})}),
      makeRowVector({makeFlatVector<int32_t>({7, 7})}),
      makeRowVector({makeFlatVector<int32_t>({8, 8})}),
      makeRowVector({makeFlatVector<int32_t>({9, 9})}),
      makeRowVector({makeFlatVector<int32_t>({10})}),
      makeRowVector({makeFlatVector<int32_t>({11})}),
      makeRowVector({makeFlatVector<int32_t>({12})}),
      makeRowVector({makeFlatVector<int32_t>({13})}),
      makeRowVector({makeFlatVector<int32_t>({14})}),
      makeRowVector({makeFlatVector<int32_t>({15})}),
      makeRowVector({makeFlatVector<int32_t>({16, 16, 16, 16})}),
  };

  testVectorGrouping({"c0"}, input, excepted, 2);
  testVectorGrouping({"c0"}, input, excepted, 3);
}

TEST_F(VectorGroupingTest, grouping) {
  auto a = ARRAY(BIGINT());
  const auto leftType =
      ROW({"c0", "c1", "c2"}, {BIGINT(), DOUBLE(), VARCHAR()});
  constexpr auto numVectors = 2;
  constexpr auto rowsPerVector = 7;
  const auto vectors = makeVectors(leftType, numVectors, rowsPerVector);
  constexpr int numSplits{5};
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < numSplits; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }
  auto splits = makeHiveConnectorSplits(splitFiles);

  const auto planNodeIdGenerator =
      std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(leftType)
                  .capturePlanNodeId(scanNodId)
                  .orderBy({"c0"}, false)
                  .vectorGrouping({"c0"}, 2)
                  .planNode();
  std::vector<RowVectorPtr> duckInputs;
  for (auto i = 0; i < numSplits; ++i) {
    for (const auto& vector : vectors) {
      duckInputs.push_back(vector);
    }
  }
  createDuckDbTable(duckInputs);
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .splits(makeHiveConnectorSplits(splitFiles))
      .assertResults("select * from tmp order by c0");

  CursorParameters cursorParams;
  cursorParams.planNode = std::move(plan);
  cursorParams.copyResult = false;
  const auto cursor = TaskCursor::create(cursorParams);
  auto& task = cursor->task();
  for (auto& split : splits) {
    task->addSplit(scanNodId, Split(std::move(split)));
  }
  task->noMoreSplits(scanNodId);

  int64_t numOutputVectors = 0;
  while (cursor->moveNext()) {
    const auto& batch = cursor->current();
    ++numOutputVectors;
    ASSERT_EQ(batch->size(), numVectors * numSplits);
  }
  ASSERT_EQ(numOutputVectors, rowsPerVector);
}

} // namespace facebook::velox::exec::test
