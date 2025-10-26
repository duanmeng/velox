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
#include "Boltrain.h"

#include <connectors/hive/HiveConnector.h>
#include <connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h>
#include <dwio/parquet/RegisterParquetReader.h>
#include <exec/tests/utils/HiveConnectorTestBase.h>
#include <folly/init/Init.h>
#include <functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <functions/prestosql/registration/RegistrationFunctions.h>
#include <parse/TypeResolver.h>

#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/Expressions.h"

DEFINE_double(
    hive_connector_executor_hw_multiplier,
    2.0,
    "Hardware multipler for hive connector.");
DEFINE_double(
    driver_cpu_executor_hw_multiplier,
    2.0,
    "Hardware multipler for driver cpu executor.");
DEFINE_int64(velox_allocator_capacity, 75L << 30, "Allocator capacity");
DEFINE_int64(velox_pool_capacity, 70L << 30, "Allocator capacity");
DEFINE_int64(
    max_coalesced_bytes,
    8L << 20,
    "Maximum size of single coalesced IO");
DEFINE_int64(load_quantum_bytes, 8L << 20, "Maximum size of load quantum");
DEFINE_string(
    max_coalesced_distance_bytes,
    "512kB",
    "Maximum distance in bytes in which coalesce will combine requests");
DEFINE_int32(
    parquet_prefetch_rowgroups,
    1,
    "Number of next row groups to "
    "prefetch. 1 means prefetch the next row group before decoding "
    "the current one");

DEFINE_int32(preferred_output_batch_rows, 100, "prefered batch rows");
DEFINE_int32(max_output_batch_rows, 100, "max batch rows");
DEFINE_bool(enable_multithread_scan, false, "Enable multithread column load");
DEFINE_bool(velox_use_mmap, false, "Use mmap allocator and arena");
DEFINE_bool(velox_check_memory_leak, false, "Check memory leak");

DEFINE_bool(
    velox_enable_left_prefetch,
    false,
    "Enable prefetch of the left side, which is false by deafult.");
DEFINE_int64(
    left_load_quantum_bytes,
    8L << 20,
    "Maximum size of left load quantum");
DEFINE_int64(
    seq_load_quantum_bytes,
    8L << 20,
    "Maximum size of sequence load quantum");
DEFINE_bool(velox_use_uid_filter, true, "Use uid filter");
DEFINE_int64(boltrain_hdfs_timeoutMs, 600'000L, "hdfs timeout ms");
DEFINE_int32(
    boltrain_task_clean_intervalS,
    5,
    "task clean interval in seconds");
DEFINE_bool(velox_copy_result, false, "Copy result");
DEFINE_bool(velox_flatten_result, true, "Flatten result");
DEFINE_bool(velox_group_by_uid, false, "Grouping by uid");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;
using namespace facebook::velox::exec::test;

namespace facebook::velox::boltrain {

Boltrain* Boltrain::getBoltrain() {
  static Boltrain inst;
  return &inst;
}

Boltrain::Boltrain()
    : cpuExecutor_(std::make_unique<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency() *
              FLAGS_driver_cpu_executor_hw_multiplier,
          std::make_shared<folly::NamedThreadFactory>("BoltrainCpuConnector"))),
      ioExecutor_(std::make_unique<folly::IOThreadPoolExecutor>(
          std::thread::hardware_concurrency() *
              FLAGS_hive_connector_executor_hw_multiplier,
          std::make_shared<folly::NamedThreadFactory>("BoltrainIoConnector"))) {
  initRegisters();
  initMemoryPools();
  initConnectors();
}

// Registers Velox functions, types, filesystems, and other components.
void Boltrain::initRegisters() {
  velox::functions::prestosql::registerAllScalarFunctions();
  velox::aggregate::prestosql::registerAllAggregateFunctions();
  velox::parse::registerTypeResolver();
  velox::parquet::registerParquetReaderFactory();
  velox::filesystems::registerHdfsFileSystem();
  velox::filesystems::registerLocalFileSystem();
}

// Init Velox memory manager and root pool for the query.
void Boltrain::initMemoryPools() {
  memory::MemoryManager::Options memOptions;
  memOptions.useMmapAllocator = FLAGS_velox_use_mmap;
  memOptions.allocatorCapacity = FLAGS_velox_allocator_capacity;
  memOptions.useMmapArena = FLAGS_velox_use_mmap;
  memOptions.mmapArenaCapacityRatio = 1;

  memory::MemoryManager::initialize(memOptions);
  rootPool_ = memory::memoryManager()->addRootPool(
      "BoltrainRoot", FLAGS_velox_pool_capacity);
  pool_ = rootPool_->addLeafChild("BoltrainLeaf");
  copyPool_ = rootPool_->addLeafChild("BoltrainCopyLeaf");
  fileMetaPool_ = rootPool_->addLeafChild("BoltrainFileMetaLeaf");
  arrowExporterPool_ = rootPool_->addLeafChild("BoltrainArrowExporterLeaf");
}

// Init Velox connectors.
void Boltrain::initConnectors() {
  auto configurationValues = std::unordered_map<std::string, std::string>();
  configurationValues[connector::hive::HiveConfig::kMaxCoalescedBytes] =
      std::to_string(FLAGS_max_coalesced_bytes);
  configurationValues[connector::hive::HiveConfig::kPrefetchRowGroups] =
      std::to_string(FLAGS_parquet_prefetch_rowgroups);
  configurationValues[connector::hive::HiveConfig::kLoadQuantum] =
      std::to_string(FLAGS_load_quantum_bytes);
  const auto properties = std::make_shared<const config::ConfigBase>(
      std::move(configurationValues));
  connector::hive::HiveConnectorFactory factory;
  const auto hiveConnector = factory.newConnector(
      kHiveConnectorId,
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
      ioExecutor_.get());
  connector::registerConnector(hiveConnector);
}

core::PlanNodePtr BoltrainPlanner::plan() {
  return nullptr;
}

void BoltrainPlanner::createLeftBuilder() {
  leftBuilder_ = PlanBuilder(planNodeIdGenerator_, pool_);
  leftBuilder_.values({});
  leftBuilder_.project({"cast(1 as bigint) as c0"});
}

void BoltrainPlanner::createRightBuilder() {
  rightBuilder_ = PlanBuilder(planNodeIdGenerator_, pool_);
  rightBuilder_.values({});
  rightBuilder_.project({"cast(1 as bigint) as c0"});
}

} // namespace facebook::velox::boltrain

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};
  boltrain::BoltrainReader reader;
  return 0;
}
