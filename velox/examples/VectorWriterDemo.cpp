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
#include <vector>

#include "velox/expression/VectorReaders.h"
#include "velox/expression/VectorWriters.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

/**
 * Kafka
 * message会有多种字段，Feature，RawFeature，Attribute/NA，需要将它们攒批生成一个
 * 带Schema的RowVector，这里Schema每个column
 * name统一称为featureName，它有特征类型和数据类型，
 * - 特征类型就是Feature，RawFeature，Attribute/NA
 * - 数据类型就是Int64List，DoubleList这样的
 *
 * 每个featureName的Velox
 * type可以通过特征类型和数据类型创建处理，这里可以做一个工厂方法，根据
 * 两类字符串的输入，返回一个Velox type
 * - 如果是Feature，是一个一层单column的ROW，name是"value"，比如ROW({"value"},
 * {ARRAY(BIGINT())})
 * - 如果是Attribute，就是一个没有任何包装的vector，比如VARCHAR()
 * -
 * 如果时RawFeature，是一个两层ROW，第一层可能有多个column，命名规则是c0，c1递增，第二层只有一个column，name是"value"，
 *     比如，ROW({"c0", "c1"}, {ROW({"value"}, {ARRAY(BIGINT())}),
 * ROW({"value"}, {ARRAY(DOUBLE())})})
 *
 *
 *
 * 需要一个工厂根据类型和数据类型，创建一个ColumnWriter，里面根据特征类型，
 * 里面包含数据类型对应的vectors和ColumnWriters
 *
 * - 对于类型是Feature，Attribute的ColumnWriter，底层只需要一个ColumnWriterImpl,
 *   该底层只需要一个ColumnWriterImpl的type和dim由数据类型确定。
 *   - Int64List => ColumnWriterImpl<int64_t, 1>
 *   - Int64Lists => ColumnWriterImpl <int64_t, 2>
 * -
 * 对于类型是RawFeature的ColumnWriter，底层可能需要多个ColumnWriterImpl，因为它的数据类型
 *   可能是一个list，比如Int64List,DoubleList，每个数据类型创建一个对应ColumnWriterImpl
 * - 每个ColumnWriter初始化时需要，会传入一个result
 * vector的type，会用该type创建一个result vector
 *   type就是上面提到的通过schema工厂方法创建的。这里可以穿入一个特征类型enum，方便解析type，路由生成结果的逻辑
 * - ColumnWriter根据result vector
 * type创建对应的ColumnWriterImpl，如果上面例子的RawFeature，就要创建两个
 *   分别对应featureName.c0.value和featureName.c1.value，然后将它们组装成featureName
 * vector
 * - 所以不同的特征类型创建的ColumnWriter在build结果的逻辑上是有差异的
 *
 */

using namespace facebook::velox;

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
  static_assert(Dim >= 0 && Dim <= 2, "Dim must be 0, 1, or 2");
  using ItemType = typename RowItemTypeTraits<Scalar, Dim>::type;
  using WriterType = typename WriterTypeTraits<Scalar, Dim>::type;

  void init(const VectorPtr& result) {
    if constexpr (Dim == 0) {
      writer_.init(*result->template as<FlatVector<ItemType>>());
    } else if constexpr (Dim == 1) {
      writer_.init(*result->template as<ArrayVector>());
    } else { // Dim == 2
      writer_.init(*result->template as<ArrayVector>());
    }
    size_ = result->size();
    offset_ = 0;
  }

  void addRow(ItemType item) {
    VELOX_CHECK_LT(offset_, size_);
    // const ItemType& item = std::any_cast<const ItemType&>(anyValue);
    writer_.setOffset(offset_);
    if constexpr (Dim == 0) {
      writer_.setOffset(offset_);
      writer_.current() = item;
      writer_.commit(true);
    } else if constexpr (Dim == 1) {
      auto& current = writer_.current();
      current.reserve(item.size());
      current.add_items(item);
      writer_.commit();
    } else { // Dim == 2
      auto& current = writer_.current();
      current.reserve(item.size());
      for (const auto& innerItems : item) {
        auto& inner = current.add_item();
        inner.reserve(innerItems.size());
        inner.add_items(innerItems);
      }
      writer_.commit();
    }
    ++offset_;
  }

  void addNullRow() {
    VELOX_CHECK_LT(offset_, size_);
    writer_.setOffset(offset_);
    if constexpr (Dim == 0) {
      writer_.commitNull();
    } else {
      writer_.setNull();
      writer_.commit();
    }
    ++offset_;
  }

  void finish() {
    VELOX_CHECK_EQ(offset_, size_);
    writer_.finish();
  }

  WriterType writer_;
  vector_size_t size_{0};
  vector_size_t offset_{0};
};

} // namespace

int main() {
  const int num_rows = 2;
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  /****************** Vector Writer **********************/

  // Define rows to write
  SelectivityVector rows{num_rows};
  ROW({"value"}, {ARRAY(BIGINT())});
  ROW({"c0", "c1"},
      {ROW({"value"}, {ARRAY(BIGINT())}), ROW({"value"}, {ARRAY(DOUBLE())})});

  // Array<Map<int,int>>
  auto type = TypeFactory<TypeKind::ARRAY>::create(BIGINT());
  auto pool = memory::memoryManager()->addLeafPool();

  // result vector
  VectorPtr result;
  BaseVector::ensureWritable(rows, type, pool.get(), result);

  // 一维数组：ARRAY(BIGINT)
  ColumnWriterImpl<int64_t, 1> w1;
  w1.init(result);
  // 一行：标量数组
  w1.addRow(std::vector<int64_t>{1, 2, 3});
  w1.addRow(std::vector<int64_t>{}); // 空数组
  w1.finish();
  LOG(ERROR) << "result: " << result->toString(0, result->size());

  // 二维数组：ARRAY(ARRAY(BIGINT))
  VectorPtr result1;
  type = TypeFactory<TypeKind::ARRAY>::create(
      TypeFactory<TypeKind::ARRAY>::create(BIGINT()));
  BaseVector::ensureWritable(rows, type, pool.get(), result1);
  ColumnWriterImpl<int64_t, 2> w2;
  w2.init(result1);
  // 一行：多个内层数组
  w2.addRow(
      std::vector<std::vector<int64_t>>{
          {10, 20}, // 第一个内层数组
          {}, // 空内层数组
          {30}});
  w2.addRow(
      std::vector<std::vector<int64_t>>{
          {100, 20}, // 第一个内层数组// 空内层数组
          {130}});
  w2.finish();
  LOG(ERROR) << "result1: " << result1->toString(0, result1->size());

  VectorPtr result2;
  auto type2 = BIGINT();
  BaseVector::ensureWritable(rows, type2, pool.get(), result2);
  ColumnWriterImpl<int64_t, 0> w3;
  w3.init(result2);
  w3.addRow(static_cast<int64_t>(2));
  w3.addNullRow();
  w3.finish();
  LOG(ERROR) << "result2: " << result2->toString(0, result2->size());

  VectorPtr result3;
  auto type3 = VARCHAR();
  BaseVector::ensureWritable(rows, type3, pool.get(), result3);
  ColumnWriterImpl<Varchar, 0> w4;
  w4.init(result3);
  w4.addRow("fd");
  w4.addNullRow();
  w4.finish();
  LOG(ERROR) << "result3: " << result3->toString(0, result3->size());

  return 0;
}

namespace {
/// backup
template <typename T>
struct IsStdVector : std::false_type {};

template <typename U, typename Alloc>
struct IsStdVector<std::vector<U, Alloc>> : std::true_type {};

template <typename T>
inline constexpr bool is_std_vector_v = IsStdVector<T>::value;

template <typename T, bool IsVec = is_std_vector_v<T>>
struct ScalarTypeTraits;

template <typename Scalar>
struct ScalarTypeTraits<Scalar, /*IsVec=*/false> {
  using type = Scalar;
};

template <typename Vec>
struct ScalarTypeTraits<Vec, /*IsVec=*/true> {
  using type = typename Vec::value_type;
};

// 映射 writer 与行入参类型
template <typename T, bool IsVec = is_std_vector_v<T>>
struct VectorWriterTypeTraits;

template <typename T>
struct VectorWriterTypeTraits<T, /*IsVec=*/false> {
  // using ElementType = typename ScalarTypeTraits<T>::type;
  using ElementType = T;
  using WriterType = exec::VectorWriter<Array<ElementType>>;
  using InputType = const std::vector<ElementType>&;
};

template <typename T>
struct VectorWriterTypeTraits<T, /*IsVec=*/true> {
  // using ElementType = typename ScalarTypeTraits<T>::type;
  using ElementType = typename T::value_type;
  using WriterType = exec::VectorWriter<Array<Array<ElementType>>>;
  using InputType = const std::vector<std::vector<ElementType>>&;
};

template <typename T>
struct ArrayVectorWriter {
  using WriterType = typename VectorWriterTypeTraits<T>::WriterType;
  using InputType = typename VectorWriterTypeTraits<T>::InputType;

  void init(const VectorPtr& result) {
    writer_.init(*result->as<ArrayVector>());
    size_ = result->size();
    offset_ = 0;
  }

  // 写入一行
  void addRow(InputType items) {
    VELOX_CHECK_LT(offset_, size_);
    writer_.setOffset(offset_);
    auto& current = writer_.current();
    current.reserve(items.size());
    if constexpr (!is_std_vector_v<T>) {
      current.add_items(items);
    } else {
      for (const auto& innerItems : items) {
        auto& inner = current.add_item();
        inner.add_items(innerItems);
      }
    }

    writer_.commit();
    ++offset_;
  }

  void addNullRow() {
    VELOX_CHECK_LT(offset_, size_);
    writer_.setOffset(offset_);
    writer_.setNull();
    writer_.commit();
    ++offset_;
  }

  void finish() {
    VELOX_CHECK_EQ(offset_, size_);
    writer_.finish();
  }

  WriterType writer_;
  vector_size_t size_{0};
  vector_size_t offset_{0};
};

template <typename T>
struct ScalarVectorWriter {
  void init(const VectorPtr& result) {
    writer_.init(*result->as<FlatVector<T>>());
    size_ = result->size();
    offset_ = 0;
  }

  // 写入一行
  void addRow(T item) {
    VELOX_CHECK_LT(offset_, size_);
    writer_.setOffset(offset_);
    writer_.current() = item;
    writer_.commit(true);
    ++offset_;
  }

  void addNullRow() {
    VELOX_CHECK_LT(offset_, size_);
    writer_.setOffset(offset_);
    writer_.commitNull();
    ++offset_;
  }

  void finish() {
    VELOX_CHECK_EQ(offset_, size_);
    writer_.finish();
  }

  exec::VectorWriter<T> writer_;
  vector_size_t size_{0};
  vector_size_t offset_{0};
};
} // namespace
