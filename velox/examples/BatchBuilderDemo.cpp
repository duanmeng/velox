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
#include <any>
#include <iostream>
#include <type_traits>
#include <variant>
#include <vector>

#include "velox/common/memory/Memory.h"
#include "velox/expression/VectorReaders.h"
#include "velox/expression/VectorWriters.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;

/// ColumnWriterImpl 和 Traits
namespace {
template <typename Scalar, int Dim>
struct RowItemTypeTraits;
template <typename Scalar>
struct RowItemTypeTraits<Scalar, 0> {
  using type = Scalar;
};
template <typename Scalar>
struct RowItemTypeTraits<Scalar, 1> {
  using type = std::vector<Scalar>;
};
template <typename Scalar>
struct RowItemTypeTraits<Scalar, 2> {
  using type = std::vector<std::vector<Scalar>>;
};
template <>
struct RowItemTypeTraits<Varchar, 0> {
  using type = StringView;
};
template <>
struct RowItemTypeTraits<Varchar, 1> {
  using type = std::vector<StringView>;
};
template <>
struct RowItemTypeTraits<Varchar, 2> {
  using type = std::vector<std::vector<StringView>>;
};

template <typename Scalar, int Dim>
struct WriterTypeTraits;
template <typename Scalar>
struct WriterTypeTraits<Scalar, 0> {
  using type = exec::VectorWriter<Scalar>;
};
template <typename Scalar>
struct WriterTypeTraits<Scalar, 1> {
  using type = exec::VectorWriter<Array<Scalar>>;
};
template <typename Scalar>
struct WriterTypeTraits<Scalar, 2> {
  using type = exec::VectorWriter<Array<Array<Scalar>>>;
};

template <typename Scalar, int Dim>
struct ColumnWriterImpl {
  using ItemType = typename RowItemTypeTraits<Scalar, Dim>::type;
  using WriterType = typename WriterTypeTraits<Scalar, Dim>::type;
  void init(const VectorPtr& result) {
    writer_.init(*result->template as<typename WriterType::vector_t>());
    offset_ = 0;
  }
  void addRow(const ItemType& item) {
    writer_.setOffset(offset_);
    if constexpr (Dim == 0) {
      writer_.current() = item;
      writer_.commit(true);
    } else if constexpr (Dim == 1) {
      auto& current = writer_.current();
      current.reserve(item.size());
      for (const auto& val : item) {
        current.add_item() = val;
      }
      writer_.commit();
    } else {
      auto& current = writer_.current();
      current.reserve(item.size());
      for (const auto& innerItems : item) {
        auto& inner = current.add_item();
        inner.reserve(innerItems.size());
        for (const auto& val : innerItems) {
          inner.add_item() = val;
        }
      }
      writer_.commit();
    }
    ++offset_;
  }
  void addNullRow() {
    writer_.setOffset(offset_);
    writer_.commitNull();
    ++offset_;
  }
  void finish() {
    writer_.finish();
  }
  WriterType writer_;
  vector_size_t offset_{0};
};
} // namespace

/// CRTP 和 SFINAE
namespace {
class ColumnWriterBase {
 public:
  virtual ~ColumnWriterBase() = default;
};

template <typename Derived>
class ColumnWriterCRTP : public ColumnWriterBase {
 public:
  void init(const VectorPtr& result) {
    static_cast<Derived*>(this)->getImpl()->init(result);
  }
  void addNullRow() {
    static_cast<Derived*>(this)->getImpl()->addNullRow();
  }
  void finish() {
    static_cast<Derived*>(this)->getImpl()->finish();
  }
};

template <typename T, int dim>
class ColumnWriter final : public ColumnWriterCRTP<ColumnWriter<T, dim>> {
 public:
  using NormalizeType =
      std::conditional_t<std::is_same_v<T, StringView>, Varchar, T>;
  using Impl = ColumnWriterImpl<NormalizeType, dim>;
  ColumnWriter() : writerImpl_(std::make_unique<Impl>()) {}
  Impl* getImpl() {
    return writerImpl_.get();
  }
  template <int D = dim>
  std::enable_if_t<D == 0, void> addRow(T item) {
    writerImpl_->addRow(item);
  }
  template <int D = dim>
  std::enable_if_t<D == 1, void> addRow(const std::vector<T>& items) {
    writerImpl_->addRow(items);
  }
  template <int D = dim>
  std::enable_if_t<D == 2, void> addRow(
      const std::vector<std::vector<T>>& items) {
    writerImpl_->addRow(items);
  }

 private:
  std::unique_ptr<Impl> writerImpl_;
};
} // namespace

/// AnyColumnWriter
namespace {
#define BOLTRAIN_DYNAMIC_SCALAR_TYPE_DISPATCH(TEMPLATE_FUNC, typeKind, ...)   \
  [&]() {                                                                     \
    switch (typeKind) {                                                       \
      case ::facebook::velox::TypeKind::BOOLEAN: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::BOOLEAN>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::INTEGER: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::INTEGER>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::BIGINT: {                             \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::BIGINT>(            \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::REAL: {                               \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::REAL>(__VA_ARGS__); \
      }                                                                       \
      case ::facebook::velox::TypeKind::DOUBLE: {                             \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::DOUBLE>(            \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARCHAR: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::VARCHAR>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARBINARY: {                          \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::VARBINARY>(         \
            __VA_ARGS__);                                                     \
      }                                                                       \
      default:                                                                \
        VELOX_FAIL(                                                           \
            "not a scalar type! kind: {}",                                    \
            ::facebook::velox::TypeKindName::toName(typeKind));               \
    }                                                                         \
  }()

/// SFINAE detector
namespace detector {
template <
    typename Default,
    typename AlwaysVoid,
    template <typename...> class Op,
    typename... Args>
struct is_detected_impl : std::false_type {};
template <template <typename...> class Op, typename... Args>
struct is_detected_impl<void, std::void_t<Op<Args...>>, Op, Args...>
    : std::true_type {};
} // namespace detector

template <template <typename...> class Op, typename... Args>
using is_detected = detector::is_detected_impl<void, void, Op, Args...>;
template <template <typename...> class Op, typename... Args>
constexpr bool is_detected_v = is_detected<Op, Args...>::value;
template <typename Writer, typename Arg>
using addRow_is_callable_t =
    decltype(std::declval<Writer&>()->addRow(std::declval<Arg>()));

class AnyColumnWriter {
 public:
  using WriterVariant = std::variant<
      std::shared_ptr<ColumnWriter<bool, 0>>,
      std::shared_ptr<ColumnWriter<bool, 1>>,
      std::shared_ptr<ColumnWriter<bool, 2>>,
      std::shared_ptr<ColumnWriter<int32_t, 0>>,
      std::shared_ptr<ColumnWriter<int32_t, 1>>,
      std::shared_ptr<ColumnWriter<int32_t, 2>>,
      std::shared_ptr<ColumnWriter<int64_t, 0>>,
      std::shared_ptr<ColumnWriter<int64_t, 1>>,
      std::shared_ptr<ColumnWriter<int64_t, 2>>,
      std::shared_ptr<ColumnWriter<float, 0>>,
      std::shared_ptr<ColumnWriter<float, 1>>,
      std::shared_ptr<ColumnWriter<float, 2>>,
      std::shared_ptr<ColumnWriter<double, 0>>,
      std::shared_ptr<ColumnWriter<double, 1>>,
      std::shared_ptr<ColumnWriter<double, 2>>,
      std::shared_ptr<ColumnWriter<StringView, 0>>,
      std::shared_ptr<ColumnWriter<StringView, 1>>,
      std::shared_ptr<ColumnWriter<StringView, 2>>>;

  AnyColumnWriter(WriterVariant writer) : writer_(std::move(writer)) {}

  void init(const VectorPtr& result) {
    std::visit([&](auto& writer) { writer->init(result); }, writer_);
  }
  void addNullRow() {
    std::visit([&](auto& writer) { writer->addNullRow(); }, writer_);
  }
  void finish() {
    std::visit([&](auto& writer) { writer->finish(); }, writer_);
  }

  template <typename T>
  void addRow(T&& arg) {
    addRowImpl(std::forward<T>(arg));
  }

 private:
  void addRowImpl(std::string& item) {
    addRowDispatch(StringView(item));
  }

  void addRowImpl(std::vector<std::string>& items) {
    std::vector<StringView> views;
    views.reserve(items.size());
    for (const auto& s : items) {
      views.emplace_back(s);
    }
    addRowDispatch(views);
  }

  void addRowImpl(std::vector<std::vector<std::string>>& items) {
    std::vector<std::vector<StringView>> views;
    views.reserve(items.size());
    for (const auto& item : items) {
      std::vector<StringView> cur;
      cur.reserve(item.size());
      for (const auto& s : item) {
        cur.emplace_back(s);
      }
      views.push_back(std::move(cur));
    }
    addRowDispatch(views);
  }

  template <typename T>
  void addRowImpl(T&& item) {
    addRowDispatch(std::forward<T>(item));
  }

  template <typename Arg>
  void addRowDispatch(Arg&& arg) {
    bool matched = false;
    std::visit(
        [&](auto& writer) {
          // C++20: (requires { writer->addRow(std::forward<Arg>(arg)); })
          if constexpr (is_detected_v<
                            addRow_is_callable_t,
                            decltype(writer),
                            Arg>) {
            writer->addRow(std::forward<Arg>(arg));
            matched = true;
          }
        },
        writer_);
    if (!matched) {
      VELOX_FAIL(
          "No matching writer found for the provided argument type: {}",
          std::string(typeid(Arg).name()));
    }
  }
  WriterVariant writer_;
};
} // namespace

/// Factories
namespace {
struct DataTypeInfo {
  TypeKind kind;
  int dim;
  std::string toString() const {
    return fmt::format("kind = {}, dim = {}", TypeKindName::toName(kind), dim);
  }
};

DataTypeInfo parseDataType(folly::StringPiece dataTypeStr) {
  int dim = 0;
  if (dataTypeStr.endsWith("List")) {
    dim = 1;
  } else if (dataTypeStr.endsWith("Lists")) {
    dim = 2;
  }

  if (dataTypeStr.startsWith("Int64") || dataTypeStr.startsWith("long") ||
      dataTypeStr.startsWith("FidV1") || dataTypeStr.startsWith("FidV2")) {
    return {TypeKind::BIGINT, dim};
  }

  if (dataTypeStr.startsWith("int")) {
    return {TypeKind::INTEGER, dim};
  }

  if (dataTypeStr.startsWith("String") || dataTypeStr.startsWith("string") ||
      dataTypeStr.startsWith("Bytes")) {
    return {TypeKind::VARCHAR, dim};
  }

  if (dataTypeStr.startsWith("Double") || dataTypeStr.startsWith("double")) {
    return {TypeKind::DOUBLE, dim};
  }

  if (dataTypeStr.startsWith("Float") || dataTypeStr.startsWith("float")) {
    return {TypeKind::REAL, dim};
  }

  VELOX_UNSUPPORTED("Unsupported data type: {}", dataTypeStr.toString());
}

TypePtr createChildVeloxType(const DataTypeInfo& info) {
  auto type = createScalarType(info.kind);
  for (int i = 0; i < info.dim; ++i) {
    type = ARRAY(type);
  }
  return type;
}

template <int dim>
struct AnyColumnWriterFactory {
  template <TypeKind kind>
  static AnyColumnWriter create() {
    using T = typename TypeTraits<kind>::NativeType;
    return AnyColumnWriter(std::make_shared<ColumnWriter<T, dim>>());
  }
};

AnyColumnWriter createWriter(const DataTypeInfo& info) {
  if (info.dim == 0) {
    return BOLTRAIN_DYNAMIC_SCALAR_TYPE_DISPATCH(
        AnyColumnWriterFactory<0>::create, info.kind);
  }
  if (info.dim == 1) {
    return BOLTRAIN_DYNAMIC_SCALAR_TYPE_DISPATCH(
        AnyColumnWriterFactory<1>::create, info.kind);
  }
  if (info.dim == 2) {
    return BOLTRAIN_DYNAMIC_SCALAR_TYPE_DISPATCH(
        AnyColumnWriterFactory<2>::create, info.kind);
  }
  VELOX_UNSUPPORTED("Unsupported dimension: {}", info.dim);
}
} // namespace

int main() {
  const int numRows = 2;
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  auto pool = memory::memoryManager()->addLeafPool();
  {
    VectorPtr result;
    {
      auto info = parseDataType("string");
      auto writer = createWriter(info);

      BaseVector::ensureWritable(
          SelectivityVector{numRows},
          createChildVeloxType(info),
          pool.get(),
          result);
      writer.init(result);
      std::string item("fd");
      for (int i = 0; i < numRows; ++i) {
        item += std::to_string(i);
      }
      writer.addRow(item);
      writer.addRow("fdfda");
      writer.finish();
    }
    LOG(ERROR) << result->toString(0, result->size());
  }

  {
    VectorPtr result;
    {
      auto info = parseDataType("StringList");
      auto writer = createWriter(info);

      BaseVector::ensureWritable(
          SelectivityVector{numRows},
          createChildVeloxType(info),
          pool.get(),
          result);
      writer.init(result);
      std::vector<std::string> items;
      for (int i = 0; i < numRows; ++i) {
        items.emplace_back("item" + std::to_string(i));
      }
      writer.addRow(items);
      writer.addNullRow();
      writer.finish();
    }
    LOG(ERROR) << result->toString(0, result->size());
  }

  {
    VectorPtr result;
    {
      auto info = parseDataType("StringLists");
      auto writer = createWriter(info);

      BaseVector::ensureWritable(
          SelectivityVector{numRows},
          createChildVeloxType(info),
          pool.get(),
          result);
      writer.init(result);
      std::vector<std::vector<std::string>> items;
      for (int i = 0; i < numRows; ++i) {
        std::vector<std::string> cur;
        for (int j = 0; j < numRows; ++j) {
          cur.emplace_back("item" + std::to_string(i + j));
        }
        items.push_back(std::move(cur));
      }
      writer.addRow(items);
      writer.addNullRow();
      writer.finish();
    }
    LOG(ERROR) << result->toString(0, result->size());
  }

  {
    auto info = parseDataType("Int64List");
    auto writer = createWriter(info);
    VectorPtr result;
    BaseVector::ensureWritable(
        SelectivityVector{numRows},
        createChildVeloxType(info),
        pool.get(),
        result);
    writer.init(result);
    writer.addRow(std::vector<int64_t>{1, 2});
    writer.addNullRow();
    writer.finish();
    LOG(ERROR) << result->toString(0, result->size());
  }

  {
    auto info = parseDataType("DoubleList");
    auto writer = createWriter(info);
    VectorPtr result;
    BaseVector::ensureWritable(
        SelectivityVector{numRows},
        createChildVeloxType(info),
        pool.get(),
        result);
    writer.init(result);
    writer.addRow(std::vector<double>{12.0, 3.2});
    writer.addNullRow();
    writer.finish();
    LOG(ERROR) << result->toString(0, result->size());
  }

  {
    auto info = parseDataType("Int64Lists");
    auto writer = createWriter(info);
    VectorPtr result;
    BaseVector::ensureWritable(
        SelectivityVector{numRows},
        createChildVeloxType(info),
        pool.get(),
        result);
    writer.init(result);
    writer.addRow(std::vector<std::vector<int64_t>>{{1, 2}, {3, 4}});
    writer.addNullRow();
    writer.finish();
    LOG(ERROR) << result->toString(0, result->size());
  }
  return 0;
}
