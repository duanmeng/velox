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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/exec/Merge.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <folly/executors/IOThreadPoolExecutor.h>

DECLARE_string(spill_merger_benchmark_name);
DECLARE_string(spill_merger_benchmark_path);
DECLARE_bool(spill_merger_benchmark_readAhead);
DECLARE_string(spill_merger_benchmark_spill_compression_kind);
DECLARE_uint32(spill_merger_benchmark_spill_executor_size);
DECLARE_uint32(spill_merger_benchmark_num_spill_vectors);
DECLARE_uint32(spill_merger_benchmark_spill_vector_size);
DECLARE_uint32(spill_merger_benchmark_num_merge_sources);
DECLARE_uint64(spill_merger_benchmark_max_spill_file_size);
DECLARE_uint32(spill_merger_benchmark_method);

namespace facebook::velox::exec::test {
/// A utility class for sort-merging data from data spilled by the `LocalMerge`
/// operator.
class AsyncSpillMerger : public std::enable_shared_from_this<AsyncSpillMerger> {
public:
    AsyncSpillMerger(
        const std::vector<SpillSortKey>& sortingKeys,
        const RowTypePtr& type,
        vector_size_t outputBatchSize,
        std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
            spillReadFilesGroup,
        const common::SpillConfig* spillConfig,
        velox::memory::MemoryPool* pool);

    void start();

    RowVectorPtr getOutput(
        std::vector<ContinueFuture>& sourceBlockingFutures,
        bool& atEnd) const;

private:
    static std::vector<std::shared_ptr<MergeSource>> createMergeSources(
        size_t numSpillSources);

    static std::vector<std::unique_ptr<BatchStream>> createBatchStreams(
        std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
            spillReadFilesGroup);

    static std::unique_ptr<SourceMerger> createSourceMerger(
          const std::vector<SpillSortKey>& sortingKeys,
          const RowTypePtr& type,
          vector_size_t outputBatchSize,
          const std::vector<std::shared_ptr<MergeSource>>& sources,
          velox::memory::MemoryPool* pool);

    void readFromSpillFileStream(size_t streamIdx);

    void scheduleAsyncSpillFileStreamReads();

    const std::vector<std::shared_ptr<MergeSource>> sources_;
    const std::vector<std::unique_ptr<BatchStream>> batchStreams_;
    const std::unique_ptr<SourceMerger> sourceMerger_;
    folly::Executor* const executor_;
    velox::memory::MemoryPool* const pool_;
};

class SpillMergerBenchmarkBase {
 public:
  SpillMergerBenchmarkBase() = default;

  /// Sets up the test.
  void setUp();

  /// Runs the test.
  void run();

  /// Prints out the measured test stats.
  void printStats() const;

  /// Cleans up the test.
  void cleanup();

 private:
  std::vector<RowVectorPtr> generateSortedVectors();

  SpillFiles generateSortedSpillFiles(
      const std::vector<RowVectorPtr>& sortedVectors);

  std::vector<std::vector<std::unique_ptr<SpillReadFile>>> generateInputs(
      size_t numStreams);

    std::unique_ptr<SpillMerger> createSpillMerger(
      std::vector<std::vector<std::unique_ptr<SpillReadFile>>> filesGroup,
      uint64_t numSpillRows) const;

  std::unique_ptr<SourceMerger> createSourceMerger(
      const std::vector<std::shared_ptr<MergeSource>>& sources,
      uint64_t outputBatchSize);

  static std::vector<std::shared_ptr<MergeSource>> createMergeSources(int num);

  static std::vector<RowVectorPtr> getOutputFromSourceMerger(
      SourceMerger* sourceMerger);

  const RowTypePtr rowType_ =
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {INTEGER(), BIGINT(), VARCHAR(), VARBINARY(), DOUBLE()});
  const std::vector<column_index_t> sortColumnIndices_{0, 1};
  const std::vector<CompareFlags> sortCompareFlags_{
      CompareFlags{.ascending = true},
      CompareFlags{.ascending = false}};
  const std::vector<SpillSortKey> sortingKeys_ =
      SpillState::makeSortingKeys(sortColumnIndices_, sortCompareFlags_);

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::memory::MemoryPool> spillMergerPool_;
  uint32_t numInputVectors_;
  uint32_t inputVectorSize_;
  uint32_t numSources_;
  std::unique_ptr<VectorFuzzer> vectorFuzzer_;
  std::vector<RowVectorPtr> rowVectors_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
  std::string spillDir_;
  common::SpillConfig spillConfig_;
  std::shared_ptr<filesystems::FileSystem> fs_;
  std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
      spillReadFilesGroups_;
  // Stats.
  uint64_t executionTimeUs_{0};
    CpuWallTiming fileOpenTiming_;
  CpuWallTiming readTiming_;
    CpuWallTiming spillTiming_;

  folly::Synchronized<common::SpillStats> spillStats_;
  tsan_atomic<bool> nonReclaimableSection_{false};
};
} // namespace facebook::velox::exec::test