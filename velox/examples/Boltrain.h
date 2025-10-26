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

#include <connectors/hive/HiveConnectorSplit.h>
#include <dwio/common/Reader.h>
#include <exec/Cursor.h>
#include <exec/tests/utils/PlanBuilder.h>
#include <folly/executors/IOThreadPoolExecutor.h>

namespace facebook::velox::boltrain {

using VeloxMemoryPoolPtr = std::shared_ptr<facebook::velox::memory::MemoryPool>;
using VeloxFilesystemPtr =
    std::shared_ptr<facebook::velox::filesystems::FileSystem>;
using VeloxRowTypePtr = facebook::velox::RowTypePtr;
using VeloxRowVectorPtr = facebook::velox::RowVectorPtr;
using VeloxVectorPtr = facebook::velox::VectorPtr;
using VeloxPlanNodePtr = facebook::velox::core::PlanNodePtr;
using VeloxPlanNodeId = facebook::velox::core::PlanNodeId;
using VeloxSplit = facebook::velox::exec::Split;
using VeloxQueryCtxPtr = std::shared_ptr<facebook::velox::core::QueryCtx>;
using VeloxQueryCtx = facebook::velox::core::QueryCtx;
using VeloxDwioReader = facebook::velox::dwio::common::Reader;
using VeloxTypePtr = std::shared_ptr<const facebook::velox::Type>;
using VeloxHiveConnectorSplit =
    facebook::velox::connector::hive::HiveConnectorSplit;
using VeloxDwioFileFormat = facebook::velox::dwio::common::FileFormat;
using VeloxTaskCursor = facebook::velox::exec::TaskCursor;
using VeloxCursorParameters = facebook::velox::exec::CursorParameters;
using VeloxTaskPtr = std::shared_ptr<facebook::velox::exec::Task>;

class Boltrain {
 public:
  static Boltrain* getBoltrain();

 private:
  std::unique_ptr<folly::CPUThreadPoolExecutor> cpuExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  VeloxMemoryPoolPtr rootPool_;
  VeloxMemoryPoolPtr fileMetaPool_;
  VeloxMemoryPoolPtr arrowExporterPool_;
  VeloxMemoryPoolPtr copyPool_;
  VeloxMemoryPoolPtr pool_;
  VeloxFilesystemPtr fileSystem_;

  Boltrain();
  // Registers Velox functions, types, filesystems, and other components.
  void initRegisters();

  // Init Velox memory manager and root pool for the query.
  void initMemoryPools();

  // Init Velox connectors.
  void initConnectors();
};

class BoltrainPlanner {
 public:
  explicit BoltrainPlanner(memory::MemoryPool* pool)
      : planNodeIdGenerator_(std::make_shared<core::PlanNodeIdGenerator>()),
        pool_(pool) {}

 private:
  core::PlanNodePtr plan();

  void createLeftBuilder();

  void createRightBuilder();

  const std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator_;
  memory::MemoryPool* pool_;
  exec::test::PlanBuilder leftBuilder_;
  exec::test::PlanBuilder rightBuilder_;
};

class BoltrainReader {
 public:
  BoltrainReader() {
    Boltrain::getBoltrain();
  }
};

} // namespace facebook::velox::boltrain
