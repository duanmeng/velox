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
/*
 * Service-style demo with initialize + process
 * - initialize(): build originalInput, evalInput (add ConstantVector), compile
 * fixed ExprSet.
 * - process(uid): update constant, eval, return dictionary-wrapped RowVector
 * over originalInput.
 */

#include <folly/init/Init.h>
#include "velox/common/memory/Memory.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class UidFilterService : public VectorTestBase {
 public:
  UidFilterService() {
    queryCtx_ = core::QueryCtx::create();
    execCtx_ = std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get());
  }

  // --------------------------
  // Lifecycle APIs
  // --------------------------

  // 1) 初始化：设置 originalInput 并编译固定 ExprSet
  // 说明：示例中直接在方法里合成一批数据；你也可以传入外部构造好的
  // RowVectorPtr。
  void initialize(RowVectorPtr data) {
    VELOX_CHECK_NOT_NULL(data, "originalInput cannot be null");
    originalInput_ = std::move(data);

    // 构建 ConstantVector（每行同值），初始值 0。长度需与 originalInput
    // 行数一致。
    const auto n = originalInput_->size();
    auto pool = pool_.get();

    reqUidBase_ = std::make_shared<FlatVector<int64_t>>(
        pool,
        BIGINT(),
        BufferPtr(nullptr),
        1,
        AlignedBuffer::allocate<int64_t>(1, pool, 0),
        std::vector<BufferPtr>{});
    reqUidBase_->set(0, 0);

    reqUidConst_ = BaseVector::wrapInConstant(n, /*index=*/0, reqUidBase_);

    // evalInput = originalInput 列 + 常量列 req_uid_const
    VELOX_CHECK_EQ(
        originalInput_->childrenSize(), 2, "expect columns: uid, val");
    auto uid = originalInput_->childAt(0);
    auto val = originalInput_->childAt(1);
    evalInput_ = makeRowVector(
        {"uid", "val", "req_uid_const"}, {uid, val, reqUidConst_});

    // 编译固定表达式：uid = req_uid_const（注意 rowType 来自 evalInput）
    exprSet_ =
        compileExpression("uid = req_uid_const", asRowType(evalInput_->type()));
  }

  // 2) 如果底层数据批次发生更换，可用该方法重置（保持 ExprSet 不变）
  void resetInput(RowVectorPtr newData) {
    VELOX_CHECK(exprSet_, "initialize must be called before resetInput");
    initialize(std::move(newData)); // 复用相同行数/列布局的构造逻辑
  }

  // 3) 每次请求处理：传入 uid，返回过滤后的 RowVector（基于 originalInput）
  RowVectorPtr process(int64_t uid) {
    VELOX_CHECK(exprSet_, "initialize must be called before process");
    VELOX_CHECK(originalInput_, "originalInput is not set");

    // 更新常量基值
    reqUidBase_->set(0, uid);

    // eval -> 布尔列
    auto boolVec = evaluate(*exprSet_, evalInput_);

    // boolean -> indices -> wrap originalInput
    return filterWithBooleanMask(originalInput_, boolVec);
  }

  // --------------------------
  // Demo driver
  // --------------------------
  void runDemo() {
    // 构造一批示例数据（uid, val）
    auto uid = makeFlatVector<int64_t>({10, 11, 10, 12, 10, 13});
    auto val = makeFlatVector<int64_t>({100, 110, 120, 130, 140, 150});
    auto data = makeRowVector({"uid", "val"}, {uid, val});

    initialize(data);

    std::cout << "\n> originalInput: " << originalInput_->toString()
              << std::endl;
    std::cout << originalInput_->toString(0, originalInput_->size())
              << std::endl;

    for (int64_t q : {10, 12, 42}) {
      auto out = process(q);
      std::cout << "\n> request uid=" << q
                << " -> filtered: " << out->toString() << std::endl;
      std::cout << out->toString(0, out->size()) << std::endl;
    }
  }

 private:
  // --------------------------
  // Helpers
  // --------------------------

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) const {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) const {
    std::vector<core::TypedExprPtr> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) const {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());
    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result;
    result.reserve(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  static RowVectorPtr filterWithBooleanMask(
      const RowVectorPtr& originalInput,
      const VectorPtr& boolVec) {
    const SelectivityVector rows(originalInput->size());
    const DecodedVector decoded(*boolVec, rows);

    std::vector<vector_size_t> keep;
    keep.reserve(originalInput->size());
    for (vector_size_t i = 0; i < originalInput->size(); ++i) {
      if (!decoded.isNullAt(i) && decoded.valueAt<bool>(i)) {
        keep.push_back(i);
      }
    }

    auto pool = originalInput->pool();
    auto size = keep.size();
    auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
    if (size > 0) {
      std::memcpy(
          indices->asMutable<vector_size_t>(),
          keep.data(),
          size * sizeof(vector_size_t));
    }

    std::vector<VectorPtr> cols;
    cols.reserve(originalInput->childrenSize());
    for (auto& c : originalInput->children()) {
      cols.push_back(BaseVector::wrapInDictionary(nullptr, indices, size, c));
    }
    return std::make_shared<RowVector>(
        pool, originalInput->type(), nullptr, size, std::move(cols));
  }

 private:
  // Context
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::unique_ptr<core::ExecCtx> execCtx_;

  // State
  RowVectorPtr originalInput_; // 被查询的数据
  RowVectorPtr evalInput_; // eval 输入（包含 req_uid_const 列）
  std::shared_ptr<FlatVector<int64_t>>
      reqUidBase_; // ConstantVector 的基向量（len=1）
  VectorPtr reqUidConst_; // ConstantVector<int64_t>
  std::unique_ptr<exec::ExprSet> exprSet_; // 固定表达式：uid = req_uid_const
};

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};

  // 注册函数与解析器（通常一次）
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  // 初始化内存管理
  memory::initializeMemoryManager(memory::MemoryManager::Options{});

  UidFilterService svc;
  svc.runDemo();
  return 0;
}
