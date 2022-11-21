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

#include <folly/container/F14Set.h>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/ComparatorUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/array.html
/// Implements the array_has_duplicates function.
template <typename T>
class ArrayHasDuplicatesFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto& arg = args[0];

    // Input can be constant or flat.
    if (arg->isConstantEncoding()) {
      auto* constantArray = arg->as<ConstantVector<ComplexType>>();
      const auto& flatArray = constantArray->valueVector();
      const auto flatIndex = constantArray->index();

      SelectivityVector singleRow(flatIndex + 1, false);
      singleRow.setValid(flatIndex, true);
      singleRow.updateBounds();

      applyFlat(singleRow, flatArray, context, result);
    } else {
      applyFlat(rows, arg, context, result);
    }
  }

 private:
  void applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto arrayVector = arg->as<ArrayVector>();
    VELOX_CHECK(arrayVector);
    auto elementsVector = arrayVector->elements();

    auto elementsRows =
        toElementRows(elementsVector->size(), rows, arrayVector);
    exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

    context.ensureWritable(rows, BOOLEAN(), result);
    auto flatResult = result->asFlatVector<bool>();

    // Process the rows: use a hashset to store the existed elements
    folly::F14FastSet<T> uniqSet;
    rows.applyToSelected([&](vector_size_t row) {
      auto size = arrayVector->sizeAt(row);
      auto offset = arrayVector->offsetAt(row);
      vector_size_t numNulls = 0;

      auto hasDup = false;
      for (vector_size_t i = offset; i < offset + size; ++i) {
        if (elements->isNullAt(i)) {
          numNulls++;
          if (numNulls == 2) {
            hasDup = true;
            break;
          }
        } else {
          T value = elements->valueAt<T>(i);
          auto it = uniqSet.find(value);
          if (it == uniqSet.end()) {
            uniqSet.insert(value);
          } else {
            hasDup = true;
            break;
          }
        }
      }

      uniqSet.clear();
      flatResult->set(row, hasDup);
    });
  }
};

// Validate number of parameters and types.
void validateType(const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_USER_CHECK_EQ(
      inputArgs.size(),
      1,
      "array_has_duplicates requires exactly one parameter");

  auto arrayType = inputArgs.front().type;
  VELOX_USER_CHECK_EQ(
      arrayType->kind(),
      TypeKind::ARRAY,
      "array_has_duplicates requires arguments of type ARRAY");
}

// Create function template based on type.
template <TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTyped(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);

  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<ArrayHasDuplicatesFunction<T>>();
}

// Create function.
std::shared_ptr<exec::VectorFunction> create(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  validateType(inputArgs);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      createTyped, elementType->kind(), inputArgs);
}

// Define function signature.
// array(T) -> array(T) where T must be bigint or varchar.
std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(bigint)")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("array(varchar)")
          .build()};
}

} // namespace

// Register function.
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_has_duplicates,
    signatures(),
    create);

} // namespace facebook::velox::functions
