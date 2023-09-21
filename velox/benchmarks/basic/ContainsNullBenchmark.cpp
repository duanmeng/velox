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
#include <gflags/gflags.h>

#include <iostream>
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_uint32(vectorSize, 1000, "vector size per batch");
DEFINE_double(nullRatio, 0.1, "null ratio");
DEFINE_uint32(iterations, 1000, "Name of iterations");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

class ContainsNull {
 public:
  explicit ContainsNull(const VectorPtr& vector)
      : typeKind_{vector->typeKind()}, decoded_{*vector} {
    auto base = decoded_.base();
    switch (typeKind_) {
      case TypeKind::ARRAY: {
        auto arrayBase = base->as<ArrayVector>();
        children_.emplace_back(
            std::make_unique<ContainsNull>(arrayBase->elements()));
        rawOffsets_ = arrayBase->rawOffsets();
        rawSizes_ = arrayBase->rawSizes();
        break;
      }
      case TypeKind::MAP: {
        auto mapBase = base->as<MapVector>();
        children_.emplace_back(
            std::make_unique<ContainsNull>(mapBase->mapKeys()));
        children_.emplace_back(
            std::make_unique<ContainsNull>(mapBase->mapValues()));
        rawOffsets_ = mapBase->rawOffsets();
        rawSizes_ = mapBase->rawSizes();
        break;
      }
      case TypeKind::ROW: {
        auto rowBase = base->as<RowVector>();
        for (const auto& child : rowBase->children()) {
          children_.emplace_back(std::make_unique<ContainsNull>(child));
        }
        break;
      }
      default:;
    }
  }

  bool containsNull(vector_size_t row) {
    VELOX_CHECK(!decoded_.isNullAt(row));
    return containsNull(row, true);
  }

 private:
  bool containsNullInternal(vector_size_t row) {
    switch (typeKind_) {
      case TypeKind::ARRAY:
        [[fallthrough]];
      case TypeKind::MAP: {
        if (decoded_.isNullAt(row)) {
          return true;
        }

        auto baseRow = decoded_.index(row);
        auto offset = rawOffsets_[baseRow];
        auto size = rawSizes_[baseRow];
        for (auto& child : children_) {
          if (child->containsNull(offset, size)) {
            return true;
          }
        }

        return false;
      }
      case TypeKind::ROW: {
        if (decoded_.isNullAt(row)) {
          return true;
        }

        auto baseRow = decoded_.index(row);
        for (auto& child : children_) {
          if (child->containsNullInternal(baseRow)) {
            return true;
          }
        }

        return false;
      }
      default:
        return decoded_.isNullAt(row);
    }
  }

  bool containsNull(vector_size_t offset, vector_size_t size) {
    for (auto row = offset; row < offset + size; ++row) {
      if (containsNullInternal(row)) {
        return true;
      }
    }

    return false;
  }

  const TypeKind typeKind_;
  DecodedVector decoded_;
  std::vector<std::unique_ptr<ContainsNull>> children_;
  const vector_size_t* rawOffsets_;
  const vector_size_t* rawSizes_;
};

class ContainsNullBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit ContainsNullBenchmark(size_t vectorSize, double nullRatio)
      : FunctionBenchmarkBase(),
        vectorSize_(vectorSize),
        nullRation_(nullRatio) {
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize_;
    opts.containerLength = 5;
    opts.nullRatio = nullRation_;
    opts.containerHasNulls = true;
    opts.dictionaryHasNulls = false;
    opts.containerVariableLength = true;
    opts.complexElementsMaxSize = 1'000;

    VectorFuzzer fuzzer(opts, pool());

    // opts.vectorSize = 1000; 1000 rows per batch
    size_t round = 1000;
    data_ =
        fuzzer.fuzz(ARRAY(MAP(INTEGER(), ROW({DOUBLE(), ARRAY(VARCHAR())}))));
  }

  // Runs a fast path over a flat vector (no decoding).
  void runDecoded() {
    ContainsNull containsNull(data_);
    for (auto i = 0; i < data_->size(); ++i) {
      if (!data_->isNullAt(i)) {
        containsNull.containsNull(i);
      }
    }
  }

  // Runs a fast path over a flat vector (no decoding).
  void runBase() {
    for (auto i = 0; i < data_->size(); ++i) {
      if (!data_->isNullAt(i)) {
        data_->containsNullAt(i);
      }
    }
  }

  const size_t vectorSize_;
  VectorPtr data_;
  double nullRation_;
};

std::unique_ptr<ContainsNullBenchmark> benchmark;

template <typename Func>
void run(Func&& func, size_t iterations = FLAGS_iterations) {
  for (auto i = 0; i < iterations; i++) {
    func();
  }
}

BENCHMARK(DecodedVectorContainsNull) {
  run([&] { benchmark->runDecoded(); });
}

BENCHMARK(BaseVectorContainsNull) {
  run([&] { benchmark->runBase(); });
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "---vectorSize=" << FLAGS_vectorSize
            << ",nullRatio = " << FLAGS_nullRatio
            << ",iterations=" << FLAGS_iterations << "---\n";
  benchmark = std::make_unique<ContainsNullBenchmark>(
      FLAGS_vectorSize, FLAGS_nullRatio);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}