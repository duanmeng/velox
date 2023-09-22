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

#pragma once
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/type/HugeInt.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox {

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
} // namespace facebook::velox