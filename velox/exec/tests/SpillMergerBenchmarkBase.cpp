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

#include "velox/exec/tests/SpillMergerBenchmarkBase.h"
#include "velox/exec/Merge.h"

#include "velox/exec/SortBuffer.h"

DEFINE_string(
    spill_merger_benchmark_name,
    "SpillMergerBenchmarkTest",
    "The name of this benchmark");
DEFINE_string(
    spill_merger_benchmark_path,
    "",
    "The directory path for spilling. e.g. with '/path/to/dir' provided, spill "
    "file like '/path/to/dir/SpillMergerBenchmarkTest-spill-0-0-0' will be "
    "created.");
DEFINE_bool(spill_merger_benchmark_readAhead, false, "Enable readAhead");
DEFINE_string(
    spill_merger_benchmark_spill_compression_kind,
    "none",
    "The compression kind to compress spill rows before write to disk");
DEFINE_uint32(
    spill_merger_benchmark_spill_executor_size,
    std::thread::hardware_concurrency(),
    "The spiller executor size in number of threads");
DEFINE_uint32(
    spill_merger_benchmark_num_spill_vectors,
    10'000,
    "The number of vectors for spilling");
DEFINE_uint32(
    spill_merger_benchmark_spill_vector_size,
    4096,
    "The number of rows per each spill vector");
DEFINE_uint32(
    spill_merger_benchmark_num_merge_sources,
    4,
    "The number of merge sources");
DEFINE_uint64(
    spill_merger_benchmark_max_spill_file_size,
    256 << 20,
    "The max spill file size");
DEFINE_uint32(spill_merger_benchmark_method, 0, "test method");

using namespace facebook::velox::memory;

namespace facebook::velox::exec::test {

void SpillMergerBenchmarkBase::setUp() {
  if (FLAGS_spill_merger_benchmark_readAhead) {
    LOG(INFO) << "Use spill_merger_benchmark_readAhead";
    filesystems::registerLocalFileSystem(filesystems::FileSystemOptions{true});
  } else {
    filesystems::registerLocalFileSystem();
  }
  rootPool_ =
      memory::memoryManager()->addRootPool(FLAGS_spill_merger_benchmark_name);
  pool_ = rootPool_->addLeafChild(fmt::format(
      "Default operator pool {}", FLAGS_spill_merger_benchmark_name));
  spillMergerPool_ = rootPool_->addLeafChild(
      fmt::format("SpillMerger pool {}", FLAGS_spill_merger_benchmark_name));
  numInputVectors_ = FLAGS_spill_merger_benchmark_num_spill_vectors;
  inputVectorSize_ = FLAGS_spill_merger_benchmark_spill_vector_size;
  numSources_ = FLAGS_spill_merger_benchmark_num_merge_sources;
  VectorFuzzer::Options options;
  options.vectorSize = inputVectorSize_;
  vectorFuzzer_ = std::make_unique<VectorFuzzer>(options, pool_.get());

  if (FLAGS_spill_merger_benchmark_spill_executor_size != 0) {
    executor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        FLAGS_spill_merger_benchmark_spill_executor_size,
        std::make_shared<folly::NamedThreadFactory>(
            FLAGS_spill_merger_benchmark_name));
  }

  if (FLAGS_spill_merger_benchmark_path.empty()) {
    tempDir_ = exec::test::TempDirectoryPath::create();
    spillDir_ = tempDir_->getPath();
  } else {
    spillDir_ = FLAGS_spill_merger_benchmark_path;
  }

  spillConfig_ = {
      [&]() -> const std::string& { return spillDir_; },
      [&](uint64_t) {},
      FLAGS_spill_merger_benchmark_name,
      FLAGS_spill_merger_benchmark_max_spill_file_size,
      2 << 20,
      1 << 20,
      executor_.get(),
      100,
      100,
      0,
      0,
      0,
      0,
      0,
      FLAGS_spill_merger_benchmark_spill_compression_kind,
      std::nullopt};

  fs_ = filesystems::getFileSystem(spillDir_, {});
  fs_->mkdir(spillDir_);

  std::vector<SpillPartitionSet> spillPartitionSets;
  spillReadFilesGroups_ = generateInputs(numSources_);
}

void SpillMergerBenchmarkBase::run() {
  uint64_t numRows = 0;
  uint64_t numBatches = 0;
  CpuWallTimer timer(timing_);
  MicrosecondTimer timer1(&executionTimeUs_);

  if (FLAGS_spill_merger_benchmark_method == 0) {
    LOG(INFO) << "Running SpillMergerBenchmark in async mode";
    const auto sources = createMergeSources(numSources_);
    std::vector<std::unique_ptr<BatchStream>> batchStreams;
    batchStreams.reserve(numSources_);
    for (auto i = 0; i < numSources_; ++i) {
      batchStreams.emplace_back(ConcatFilesSpillBatchStream::create(
          std::move(spillReadFilesGroups_[i])));
    }
    createFileStreamAsyncProducers(batchStreams, sources);
    const auto sourceMerger = createSourceMerger(sources, inputVectorSize_);
    std::vector<ContinueFuture> sourceBlockingFutures;
    for (;;) {
      sourceMerger->isBlocked(sourceBlockingFutures);
      if (!sourceBlockingFutures.empty()) {
        auto future = std::move(sourceBlockingFutures.back());
        sourceBlockingFutures.pop_back();
        future.wait();
        continue;
      }
      bool atEnd = false;
      auto output = sourceMerger->getOutput(sourceBlockingFutures, atEnd);
      if (output != nullptr) {
        ++numBatches;
        numRows += output->size();
      }
      if (atEnd) {
        break;
      }
    }
  } else if (FLAGS_spill_merger_benchmark_method == 1) {
    LOG(INFO) << "Running SpillMergerBenchmark in sync mode sourcestream";
    std::vector<std::unique_ptr<MergeSource>> sources;
    sources.reserve(spillReadFilesGroups_.size());
    for (auto& spillReadFilesGroup : spillReadFilesGroups_) {
      sources.emplace_back(MergeSource::createSpillMergeSource(
          ConcatFilesSpillBatchStream::create(std::move(spillReadFilesGroup))));
    }
    std::vector<std::unique_ptr<SourceStream>> sourceStreams;
    sourceStreams.reserve(sources.size());
    for (const auto& source : sources) {
      sourceStreams.push_back(std::make_unique<SourceStream>(
          source.get(), sortingKeys_, inputVectorSize_));
    }
    auto sourceMerger = std::make_unique<SourceMerger>(
        rowType_,
        inputVectorSize_,
        std::move(sourceStreams),
        spillMergerPool_.get());
    std::vector<ContinueFuture> sourceBlockingFutures;
    for (;;) {
      sourceMerger->isBlocked(sourceBlockingFutures);
      bool atEnd = false;
      auto output = sourceMerger->getOutput(sourceBlockingFutures, atEnd);
      if (output != nullptr) {
        ++numBatches;
        numRows += output->size();
      }
      VELOX_CHECK(sourceBlockingFutures.empty());
      if (atEnd) {
        break;
      }
    }
  } else {
    LOG(INFO) << "Running SpillMergerBenchmark in sync mode spillstream";
    const auto spillMerger = createSpillMerger(
        std::move(spillReadFilesGroups_),
        numSources_ * numInputVectors_ * inputVectorSize_);
    for (;;) {
      auto output = spillMerger->getOutput(inputVectorSize_);
      if (output == nullptr) {
        break;
      }
      ++numBatches;
      numRows += output->size();
    }
  }

  LOG(INFO) << "numRows: " << numRows << ", numBatches: " << numBatches;
}

void SpillMergerBenchmarkBase::cleanup() {
  LOG(INFO) << "Remove spill dir: " << spillDir_;
  fs_->rmdir(spillDir_);
}

void SpillMergerBenchmarkBase::printStats() const {
  LOG(INFO) << "total execution time: " << succinctMicros(executionTimeUs_);
  LOG(INFO) << "total wall time: " << succinctNanos(timing_.wallNanos);
  LOG(INFO) << "total cpu time: " << succinctNanos(timing_.cpuNanos);

  LOG(INFO) << numInputVectors_ << " vectors each with " << inputVectorSize_
            << " rows have been processed";
  const auto memStats = spillMergerPool_->stats();
  LOG(INFO) << "peak memory usage[" << succinctBytes(memStats.peakBytes)
            << "] cumulative memory usage["
            << succinctBytes(memStats.cumulativeBytes) << "]";
}

std::vector<RowVectorPtr> SpillMergerBenchmarkBase::generateSortedVectors() {
  const auto sortBuffer = std::make_unique<SortBuffer>(
      rowType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      common::PrefixSortConfig{},
      nullptr,
      nullptr);
  for (auto j = 0; j < numInputVectors_; ++j) {
    sortBuffer->addInput(vectorFuzzer_->fuzzRow(rowType_));
  }
  sortBuffer->noMoreInput();
  std::vector<RowVectorPtr> sortedVectors;
  sortedVectors.reserve(numInputVectors_);
  for (auto i = 0; i < numInputVectors_; ++i) {
    sortedVectors.emplace_back(sortBuffer->getOutput(inputVectorSize_));
  }
  return sortedVectors;
}

SpillFiles SpillMergerBenchmarkBase::generateSortedSpillFiles(
    const std::vector<RowVectorPtr>& sortedVectors) {
  const auto spiller = std::make_unique<MergeSpiller>(
      rowType_,
      std::nullopt,
      HashBitRange{},
      sortingKeys_,
      &spillConfig_,
      &spillStats_);
  for (const auto& vector : sortedVectors) {
    spiller->spill(SpillPartitionId(0), vector);
  }
  SpillPartitionSet spillPartitionSet;
  spiller->finishSpill(spillPartitionSet);
  VELOX_CHECK_EQ(spillPartitionSet.size(), 1);
  return spillPartitionSet.cbegin()->second->files();
}

std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
SpillMergerBenchmarkBase::generateInputs(size_t numStreams) {
  std::vector<std::vector<RowVectorPtr>> totalVectors;
  std::vector<std::vector<std::unique_ptr<SpillReadFile>>> spillReadFilesGroups;
  for (auto i = 1; i <= numStreams; ++i) {
    const auto vectors = generateSortedVectors();
    const auto spillFiles = generateSortedSpillFiles(vectors);
    std::vector<std::unique_ptr<SpillReadFile>> spillReadFiles;
    spillReadFiles.reserve(spillFiles.size());
    for (const auto& spillFile : spillFiles) {
      spillReadFiles.emplace_back(SpillReadFile::create(
          spillFile, spillConfig_.readBufferSize, pool_.get(), &spillStats_));
    }
    spillReadFilesGroups.emplace_back(std::move(spillReadFiles));
  }
  return spillReadFilesGroups;
}

std::unique_ptr<SpillMerger> SpillMergerBenchmarkBase::createSpillMerger(
    std::vector<std::vector<std::unique_ptr<SpillReadFile>>> filesGroup,
    uint64_t numSpillRows) const {
  return std::make_unique<SpillMerger>(
      rowType_, numSpillRows, std::move(filesGroup), spillMergerPool_.get());
}

folly::Future<folly::Unit> SpillMergerBenchmarkBase::produceAsync(
    MergeSource* mergeSource,
    BatchStream* batchStream) const {
  ContinueFuture future;
  RowVectorPtr vector;
  if (!batchStream->nextBatch(vector)) {
    const auto reason = mergeSource->enqueue(nullptr, &future);
    return folly::makeFuture();
  }
  mergeSource->enqueue(vector, &future);
  return std::move(future)
      .via(executor_.get())
      .thenValue([this, mergeSource, batchStream](folly::Unit) {
        return produceAsync(mergeSource, batchStream);
      })
      .thenError(folly::tag_t<std::exception>{}, [](const std::exception& e) {
        VELOX_FAIL(e.what());
      });
}

void SpillMergerBenchmarkBase::createFileStreamAsyncProducers(
    const std::vector<std::unique_ptr<BatchStream>>& batchStreams,
    const std::vector<std::shared_ptr<MergeSource>>& sources) const {
  for (auto i = 0; i < batchStreams.size(); ++i) {
    executor_->add(
        [&, i]() { produceAsync(sources[i].get(), batchStreams[i].get()); });
  }
}

std::vector<std::shared_ptr<MergeSource>>
SpillMergerBenchmarkBase::createMergeSources(int num) {
  std::vector<std::shared_ptr<MergeSource>> sources;
  sources.reserve(num);
  for (auto i = 0; i < num; ++i) {
    sources.push_back(MergeSource::createLocalMergeSource());
  }
  for (const auto& source : sources) {
    source->start();
  }
  return sources;
}

std::unique_ptr<SourceMerger> SpillMergerBenchmarkBase::createSourceMerger(
    const std::vector<std::shared_ptr<MergeSource>>& sources,
    uint64_t outputBatchSize) {
  std::vector<std::unique_ptr<SourceStream>> sourceStreams;
  for (const auto& source : sources) {
    sourceStreams.push_back(std::make_unique<SourceStream>(
        source.get(), sortingKeys_, outputBatchSize));
  }
  return std::make_unique<SourceMerger>(
      rowType_,
      outputBatchSize,
      std::move(sourceStreams),
      spillMergerPool_.get());
}

std::vector<RowVectorPtr> SpillMergerBenchmarkBase::getOutputFromSourceMerger(
    SourceMerger* sourceMerger) {
  std::vector<ContinueFuture> sourceBlockingFutures;
  std::vector<RowVectorPtr> results;
  for (;;) {
    sourceMerger->isBlocked(sourceBlockingFutures);
    if (!sourceBlockingFutures.empty()) {
      auto future = std::move(sourceBlockingFutures.back());
      sourceBlockingFutures.pop_back();
      future.wait();
      continue;
    }

    bool atEnd = false;
    auto output = sourceMerger->getOutput(sourceBlockingFutures, atEnd);
    if (output != nullptr) {
      results.emplace_back(std::move(output));
    }
    if (atEnd) {
      break;
    }
  }
  return results;
}

} // namespace facebook::velox::exec::test
