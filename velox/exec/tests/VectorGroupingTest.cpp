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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

class VectorGroupingTest : public OperatorTestBase {
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

} // namespace facebook::velox::exec::test
