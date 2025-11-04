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

#include <folly/container/F14Map.h>
#include <folly/init/Init.h>
#include "velox/common/memory/Memory.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::velox::exec::test {
class VeloxVetorIndicesDemo : public VectorTestBase {
 public:
  VeloxVetorIndicesDemo() = default;

  using IndexMap = folly::F14FastMap<int64_t, std::vector<vector_size_t>>;

  void initialize(RowVectorPtr input, int uidChannel = 0) {
    VELOX_CHECK_NOT_NULL(input);
    VELOX_CHECK_GE(uidChannel, 0);
    input_ = std::move(input);
    uidChannel_ = uidChannel;
    index_ = buildIndex(input_, uidChannel_);
  }

  void resetInput(RowVectorPtr newInput) {
    initialize(std::move(newInput), uidChannel_);
  }

  RowVectorPtr process(int64_t uid) const {
    VELOX_CHECK(input_);
    return wrapByUid(input_, index_, uid);
  }

  void runDemo() {
    auto uidCol = makeFlatVector<int64_t>({10, 11, 10, 12, 10, 13});
    auto valCol = makeFlatVector<int64_t>({100, 110, 120, 130, 140, 150});
    const auto input =
        makeRowVector({"uid", "val"}, {std::move(uidCol), std::move(valCol)});
    initialize(input);
    LOG(ERROR) << "Input " << input_->toString(0, input_->size()) << std::endl;

    for (const auto uid : {10, 12, 42}) {
      const auto out = process(uid);
      if (out == nullptr) {
        LOG(ERROR) << "No output for uid " << uid << std::endl;
        continue;
      }
      for (const auto& child : out->children()) {
        VELOX_CHECK_EQ(child->encoding(), VectorEncoding::Simple::DICTIONARY);
      }

      LOG(ERROR) << "Request uid=" << uid
                 << " -> filtered: " << out->toString(0, out->size());
    }
  }

 private:
  static IndexMap buildIndex(const RowVectorPtr& input, int uidChannel) {
    const SelectivityVector rows(input->size());
    const auto uidVec = input->childAt(uidChannel);
    // No need to decode if you can ensure that input is flat.
    const DecodedVector dec(*uidVec, rows);
    IndexMap map;
    map.reserve(input->size());
    for (vector_size_t i = 0; i < input->size(); ++i) {
      if (dec.isNullAt(i)) {
        continue;
      }
      auto u = dec.valueAt<int64_t>(i);
      map[u].push_back(i);
    }
    return map;
  }

  static RowVectorPtr
  wrapByUid(const RowVectorPtr& input, const IndexMap& map, int64_t targetUid) {
    const auto it = map.find(targetUid);
    if (it == map.end()) {
      return nullptr;
    }
    return wrapByIndices(input, it->second);
  }

  static RowVectorPtr wrapByIndices(
      const RowVectorPtr& input,
      const std::vector<vector_size_t>& idx) {
    auto pool = input->pool();
    const auto size = idx.size();
    VELOX_CHECK_GT(size, 0);
    const auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
    std::memcpy(
        indices->asMutable<vector_size_t>(),
        idx.data(),
        size * sizeof(vector_size_t));
    std::vector<VectorPtr> cols;
    cols.reserve(input->childrenSize());
    for (const auto& child : input->children()) {
      cols.push_back(
          BaseVector::wrapInDictionary(nullptr, indices, size, child));
    }
    return std::make_shared<RowVector>(
        pool, input->type(), nullptr, size, std::move(cols));
  }

  RowVectorPtr input_;
  IndexMap index_;
  int uidChannel_{0};
};
} // namespace facebook::velox::exec::test

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  exec::test::VeloxVetorIndicesDemo demo;
  demo.runDemo();
  return 0;
}
