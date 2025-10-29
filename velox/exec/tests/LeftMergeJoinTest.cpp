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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "folly/experimental/EventCount.h"
#include "utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::test {
class LeftMergeJoinTest : public HiveConnectorTestBase {
 protected:
  std::vector<RowVectorPtr> generateInput(const std::vector<VectorPtr>& keys) {
    std::vector<RowVectorPtr> data;
    data.reserve(keys.size());
    vector_size_t startRow = 0;

    for (const auto& key : keys) {
      auto payload = makeFlatVector<int32_t>(
          key->size(), [startRow](auto row) { return (startRow + row) * 10; });
      auto constPayload = BaseVector::createConstant(
          DOUBLE(), (double)startRow, key->size(), pool());
      auto dictPayload = BaseVector::wrapInDictionary(
          {},
          makeIndicesInReverse(key->size()),
          key->size(),
          makeFlatVector<std::string>(key->size(), [startRow](auto row) {
            return fmt::format("{}", (startRow + row) * 10);
          }));
      data.push_back(makeRowVector({key, payload, constPayload, dictPayload}));
      startRow += key->size();
    }
    return data;
  }
};
} // namespace facebook::velox::exec::test

TEST_F(LeftMergeJoinTest, basic) {
  auto left = std::vector<RowVectorPtr>{
      makeRowVector({
          makeFlatVector<int32_t>({1, 1}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector(
          {makeFlatVector<int32_t>({2, 2, 2}),
           makeFlatVector<std::string>({"a", "b", "c"})}),
      makeRowVector({
          makeFlatVector<int32_t>({3, 3}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({4, 4, 4}),
          makeFlatVector<std::string>({"a", "b", "c"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({6, 6, 6}),
          makeFlatVector<std::string>({"a", "b", "c"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({7, 7}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({8, 8}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({9, 9}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({10}),
          makeFlatVector<std::string>({"a"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({11}),
          makeFlatVector<std::string>({"a"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({16, 16, 16, 16}),
          makeFlatVector<std::string>({"a", "b", "a", "b"}),
      }),
  };

  auto right = std::vector<RowVectorPtr>{
      makeRowVector({
          makeFlatVector<std::string>({"a", "b"}),
          makeFlatVector<int32_t>({1, 2}),
      }),
      makeRowVector({
          makeFlatVector<std::string>({"a", "b", "c", "d"}),
          makeFlatVector<int32_t>({7, 9, 10, 11}),
      }),
      makeRowVector({
          makeFlatVector<std::string>({"a", "b", "c"}),
          makeFlatVector<int32_t>({15, 16, 17}),
      }),
      makeRowVector({
          makeFlatVector<std::string>({"a", "b", "c"}),
          makeFlatVector<int32_t>({18, 19, 20}),
      }),
  };
  createDuckDbTable("t", left);
  createDuckDbTable("u", right);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .values(std::move(left))
                        .leftMergeJoin(
                            {"c0"},
                            {"r1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(std::move(right))
                                .project({"c0 as r0", "c1 as r1"})
                                .planNode(),
                            {"c0", "c1", "r0"})
                        .planNode();
  LOG(ERROR) << plan->toString(true, true);
  const auto result = AssertQueryBuilder(plan).copyResults(pool());

  LOG(ERROR) << "Plan results: " << result->toString(0, result->size());

  AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .assertResults(
          "select t.c0 as c0, t.c1 as c1, u.c0 as r0 from t left join u on t.c0 = u.c1");
}

TEST_F(LeftMergeJoinTest, leftEndFirst) {
  auto left = std::vector<RowVectorPtr>{
      makeRowVector({
          makeFlatVector<int32_t>({1, 1}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector(
          {makeFlatVector<int32_t>({2, 2, 2}),
           makeFlatVector<std::string>({"a", "b", "c"})}),
      makeRowVector({
          makeFlatVector<int32_t>({3, 3}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({4, 4, 4}),
          makeFlatVector<std::string>({"a", "b", "c"}),
      }),
  };

  auto right = std::vector<RowVectorPtr>{
      makeRowVector({
          makeFlatVector<std::string>({"a", "b"}),
          makeFlatVector<int32_t>({1, 2}),
      }),
      makeRowVector({
          makeFlatVector<std::string>({"a", "b", "c", "d"}),
          makeFlatVector<int32_t>({3, 4, 5, 6}),
      }),
      makeRowVector({
          makeFlatVector<std::string>({"a", "b", "c"}),
          makeFlatVector<int32_t>({15, 16, 17}),
      }),
      makeRowVector({
          makeFlatVector<std::string>({"a", "b", "c"}),
          makeFlatVector<int32_t>({18, 19, 20}),
      }),
  };
  createDuckDbTable("t", left);
  createDuckDbTable("u", right);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .values(std::move(left))
                        .leftMergeJoin(
                            {"c0"},
                            {"r1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(std::move(right))
                                .project({"c0 as r0", "c1 as r1"})
                                .planNode(),
                            {"c0", "c1", "r0"})
                        .planNode();
  LOG(ERROR) << plan->toString(true, true);
  const auto result = AssertQueryBuilder(plan).copyResults(pool());

  LOG(ERROR) << "Plan results: " << result->toString(0, result->size());

  AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .assertResults(
          "select t.c0 as c0, t.c1 as c1, u.c0 as r0 from t left join u on t.c0 = u.c1");
}

TEST_F(LeftMergeJoinTest, rightEndFirst) {
  auto left = std::vector<RowVectorPtr>{
      makeRowVector({
          makeFlatVector<int32_t>({1, 1}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector(
          {makeFlatVector<int32_t>({2, 2, 2}),
           makeFlatVector<std::string>({"a", "b", "c"})}),
      makeRowVector({
          makeFlatVector<int32_t>({3, 3}),
          makeFlatVector<std::string>({"a", "b"}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({4, 4, 4}),
          makeFlatVector<std::string>({"a", "b", "c"}),
      }),
  };

  auto right = std::vector<RowVectorPtr>{
      makeRowVector({
          makeFlatVector<std::string>({"a", "b"}),
          makeFlatVector<int32_t>({1, 2}),
      }),
  };
  createDuckDbTable("t", left);
  createDuckDbTable("u", right);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .values(std::move(left))
                        .leftMergeJoin(
                            {"c0"},
                            {"r1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(std::move(right))
                                .project({"c0 as r0", "c1 as r1"})
                                .planNode(),
                            {"c0", "c1", "r0"})
                        .planNode();
  LOG(ERROR) << plan->toString(true, true);
  const auto result = AssertQueryBuilder(plan).copyResults(pool());

  LOG(ERROR) << "Plan results: " << result->toString(0, result->size());

  AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .assertResults(
          "select t.c0 as c0, t.c1 as c1, u.c0 as r0 from t left join u on t.c0 = u.c1");
}

TEST_F(LeftMergeJoinTest, grouping) {
  const auto scanType =
      ROW({"l_c0", "l_c1", "l_c2", "l_c3"},
          {BIGINT(), DOUBLE(), VARCHAR(), ARRAY(BIGINT())});
  constexpr auto numVectors = 10;
  constexpr auto rowsPerVector = 100;
  const auto vectors = makeVectors(scanType, numVectors, rowsPerVector);
  constexpr int numSplits{3};
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
  auto leftGroupingPlan = PlanBuilder(planNodeIdGenerator)
                              .tableScan(scanType)
                              .capturePlanNodeId(scanNodId)
                              .filter("l_c0 is not null")
                              .orderBy({"l_c0"}, false)
                              .vectorGrouping({"l_c0"}, 2)
                              .planNode();

  std::vector<RowVectorPtr> duckInputs;
  for (auto i = 0; i < numSplits; ++i) {
    for (const auto& vector : vectors) {
      duckInputs.push_back(vector);
    }
  }
  createDuckDbTable(duckInputs);
  AssertQueryBuilder(leftGroupingPlan, duckDbQueryRunner_)
      .splits(makeHiveConnectorSplits(splitFiles))
      .assertResults("select * from tmp where l_c0 is not null order by l_c0");

  // Generate left side inputs.
  CursorParameters cursorParams;
  cursorParams.planNode = std::move(leftGroupingPlan);
  cursorParams.copyResult = false;
  cursorParams.queryConfigs = {
      {"max_output_batch_rows", "10"}, {"preferred_output_batch_rows", "10"}};
  auto cursor = TaskCursor::create(cursorParams);
  auto& task = cursor->task();
  for (auto& split : splits) {
    task->addSplit(scanNodId, Split(std::move(split)));
  }
  task->noMoreSplits(scanNodId);

  std::vector<RowVectorPtr> leftGroupingInputs;
  while (cursor->moveNext()) {
    const auto& batch = cursor->current();
    leftGroupingInputs.push_back(batch);
  }

  // Generate right side inputs.
  const auto rightInputVectors =
      makeVectors(scanType, numVectors, rowsPerVector);
  const auto vectors1 =
      makeVectors(scanType, numVectors * 2, rowsPerVector - 20);
  const auto rightDistinctPlan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors1)
          .filter("l_c0 is not null")
          .project({"l_c1 as r_c0", "l_c0 as r_c1"})
          .orderBy({"r_c1"}, false)
          .singleAggregation({"r_c0", "r_c1"}, {"count(1)"})
          .project({"r_c0", "r_c1"})
          .planNode();

  CursorParameters rightCursorParams;
  rightCursorParams.planNode = rightDistinctPlan;
  rightCursorParams.copyResult = false;
  rightCursorParams.queryConfigs = {
      {"max_output_batch_rows", "5"}, {"preferred_output_batch_rows", "5"}};
  const auto rightCursor = TaskCursor::create(rightCursorParams);
  std::vector<RowVectorPtr> rightDistinctInputs;
  while (rightCursor->moveNext()) {
    const auto& batch = rightCursor->current();
    rightDistinctInputs.push_back(batch);
  }

  // Assert the results.
  const auto leftJoinPlan =
      PlanBuilder(planNodeIdGenerator)
          .values(leftGroupingInputs)
          .leftMergeJoin(
              {"l_c0"},
              {"r_c1"},
              PlanBuilder(planNodeIdGenerator)
                  .values(rightDistinctInputs)
                  .planNode(),
              {"l_c0", "l_c1", "l_c2", "l_c3", "r_c0", "r_c1"})
          .planNode();
  createDuckDbTable("l", leftGroupingInputs);
  createDuckDbTable("r", rightDistinctInputs);
  AssertQueryBuilder(leftJoinPlan, duckDbQueryRunner_)
      .assertResults("select * from l left join r on l.l_c0 = r.r_c1");

  CursorParameters leftJoinParams;
  leftJoinParams.planNode = leftJoinPlan;
  leftJoinParams.copyResult = false;
  leftJoinParams.queryConfigs = {
      {"max_output_batch_rows", "5"}, {"preferred_output_batch_rows", "5"}};

  // The sort key must be the same in a batch, and less than the following
  // batch.
  int64_t lastKey = 0;
  int64_t numOutputBatches = 0;
  bool isFirst = true;
  const auto leftJoinCursor = TaskCursor::create(leftJoinParams);
  auto idx = 0;
  while (leftJoinCursor->moveNext()) {
    const auto& leftInput = leftGroupingInputs[idx++];
    const auto& batch = leftJoinCursor->current();
    ASSERT_EQ(leftInput->size(), batch->size());
    ++numOutputBatches;
    ASSERT_EQ(batch->childAt(4)->encoding(), VectorEncoding::Simple::CONSTANT);
    ASSERT_EQ(batch->childAt(5)->encoding(), VectorEncoding::Simple::CONSTANT);
    auto keyChild = batch->childAt(0);
    auto intKeyChild = leftInput->childAt(0);
    BaseVector::flattenVector(keyChild);
    BaseVector::flattenVector(intKeyChild);
    auto flatVector = keyChild->asFlatVector<int64_t>();
    auto inputFlatVector = intKeyChild->asFlatVector<int64_t>();
    ASSERT(flatVector != nullptr);
    ASSERT(inputFlatVector != nullptr);
    auto cur = flatVector->valueAt(0);
    auto curInput = inputFlatVector->valueAt(0);
    ASSERT_EQ(cur, curInput);
    for (auto i = 1; i < flatVector->size(); ++i) {
      ASSERT_EQ(cur, flatVector->valueAt(i));
      ASSERT_EQ(cur, inputFlatVector->valueAt(i));
    }
    if (!isFirst) {
      ASSERT_GT(cur, lastKey);
    }
    lastKey = cur;
    isFirst = false;
  }
  ASSERT_EQ(numOutputBatches, leftGroupingInputs.size());
}

TEST_F(LeftMergeJoinTest, sortMergeJoin) {
  for (auto factor = 1; factor < 4; ++factor) {
    LOG(ERROR) << "Factor: " << factor;
    const auto scanType =
        ROW({"l_c0", "l_c1", "l_c2", "l_c3"},
            {BIGINT(), DOUBLE(), VARCHAR(), ARRAY(BIGINT())});
    constexpr auto numVectors = 30;
    constexpr auto rowsPerVector = 50;
    const auto vectors = makeVectors(scanType, numVectors, rowsPerVector);
    constexpr int numSplits{3};
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

    std::shared_ptr<TempFilePath> rightSplitFile = TempFilePath::create();
    const auto vectors1 = makeVectors(
        scanType,
        numVectors * factor,
        rowsPerVector * (factor > 2 ? factor - 1 : factor));
    writeToFile(rightSplitFile->getPath(), vectors1);
    core::PlanNodeId rightScanNodeId;
    auto rightSplits = makeHiveConnectorSplits({rightSplitFile});

    const auto leftJoinPlan =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(scanType)
            .capturePlanNodeId(scanNodId)
            .filter("l_c0 is not null")
            .orderBy({"l_c0"}, false)
            .vectorGrouping({"l_c0"}, 2)
            .leftMergeJoin(
                {"l_c0"},
                {"r_c1"},
                PlanBuilder(planNodeIdGenerator)
                    .tableScan(scanType)
                    .capturePlanNodeId(rightScanNodeId)
                    .filter("l_c0 is not null")
                    .project({"l_c1 as r_c0", "l_c0 as r_c1"})
                    .orderBy({"r_c1"}, false)
                    .singleAggregation({"r_c0", "r_c1"}, {"count(1)"})
                    .project({"r_c0", "r_c1"})
                    .planNode(),
                {"l_c0", "l_c1", "l_c2", "l_c3", "r_c0", "r_c1"})
            .planNode();

    // The sort key must be the same in a batch, and less than the following
    // batch.
    CursorParameters cursorParams;
    cursorParams.planNode = std::move(leftJoinPlan);
    cursorParams.copyResult = false;
    cursorParams.queryConfigs = {
        {"max_output_batch_rows", "10"}, {"preferred_output_batch_rows", "10"}};
    auto cursor = TaskCursor::create(cursorParams);
    auto& task = cursor->task();
    for (auto& split : splits) {
      task->addSplit(scanNodId, Split(std::move(split)));
    }
    for (auto& split : rightSplits) {
      task->addSplit(rightScanNodeId, Split(std::move(split)));
    }
    task->noMoreSplits(scanNodId);
    task->noMoreSplits(rightScanNodeId);

    int64_t lastKey = 0;
    int64_t numOutputBatches = 0;
    bool isFirst = true;
    while (cursor->moveNext()) {
      const auto& batch = cursor->current();
      ++numOutputBatches;
      ASSERT_EQ(
          batch->childAt(4)->encoding(), VectorEncoding::Simple::CONSTANT);
      ASSERT_EQ(
          batch->childAt(5)->encoding(), VectorEncoding::Simple::CONSTANT);
      auto keyChild = batch->childAt(0);
      BaseVector::flattenVector(keyChild);
      auto flatVector = keyChild->asFlatVector<int64_t>();
      ASSERT(flatVector != nullptr);
      auto cur = flatVector->valueAt(0);
      for (auto i = 1; i < flatVector->size(); ++i) {
        ASSERT_EQ(cur, flatVector->valueAt(i));
      }
      if (!isFirst) {
        ASSERT_GT(cur, lastKey);
      }
      lastKey = cur;
      isFirst = false;
    }
  }
}
