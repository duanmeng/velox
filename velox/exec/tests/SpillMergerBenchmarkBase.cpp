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

AsyncSpillMerger::AsyncSpillMerger(
    const std::vector<SpillSortKey>& sortingKeys,
    const RowTypePtr& type,
    vector_size_t outputBatchSize,
    std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
        spillReadFilesGroup,
    const common::SpillConfig* spillConfig,
    velox::memory::MemoryPool* pool)
    : sources_(createMergeSources(spillReadFilesGroup.size())),
      batchStreams_(createBatchStreams(std::move(spillReadFilesGroup))),
      sourceMerger_(createSourceMerger(
          sortingKeys,
          type,
          outputBatchSize,
          sources_,
          pool)),
      executor_(spillConfig->executor),
      pool_(pool) {}

void AsyncSpillMerger::start() {
  VELOX_CHECK_NOT_NULL(
      executor_,
      "SpillMerge require configure executor to run async spill file stream producer");
  scheduleAsyncSpillFileStreamReads();
}

RowVectorPtr AsyncSpillMerger::getOutput(
    std::vector<ContinueFuture>& sourceBlockingFutures,
    bool& atEnd) const {
  sourceMerger_->isBlocked(sourceBlockingFutures);
  if (!sourceBlockingFutures.empty()) {
    return nullptr;
  }
  return sourceMerger_->getOutput(sourceBlockingFutures, atEnd);
}

std::vector<std::shared_ptr<MergeSource>> AsyncSpillMerger::createMergeSources(
    size_t numSpillSources) {
  std::vector<std::shared_ptr<MergeSource>> sources;
  sources.reserve(numSpillSources);
  for (auto i = 0; i < numSpillSources; ++i) {
    sources.push_back(MergeSource::createLocalMergeSource());
    }
  for (const auto& source : sources) {
    source->start();
  }
  return sources;
}

std::vector<std::unique_ptr<BatchStream>> AsyncSpillMerger::createBatchStreams(
    std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
        spillReadFilesGroup) {
  const auto numStreams = spillReadFilesGroup.size();
  std::vector<std::unique_ptr<BatchStream>> batchStreams;
  batchStreams.reserve(numStreams);
  for (auto i = 0; i < numStreams; ++i) {
    batchStreams.emplace_back(
        ConcatFilesSpillBatchStream::create(std::move(spillReadFilesGroup[i])));
  }
  return batchStreams;
}

std::unique_ptr<SourceMerger> AsyncSpillMerger::createSourceMerger(
    const std::vector<SpillSortKey>& sortingKeys,
    const RowTypePtr& type,
    vector_size_t outputBatchSize,
    const std::vector<std::shared_ptr<MergeSource>>& sources,
    velox::memory::MemoryPool* pool) {
  std::vector<std::unique_ptr<SourceStream>> streams;
  streams.reserve(sources.size());
  for (const auto& source : sources) {
    streams.push_back(std::make_unique<SourceStream>(
        source.get(), sortingKeys, outputBatchSize));
  }
  return std::make_unique<SourceMerger>(
      type, outputBatchSize, std::move(streams), pool);
}

void AsyncSpillMerger::readFromSpillFileStream(size_t streamIdx) {
  RowVectorPtr vector;
  ContinueFuture future;
  if (!batchStreams_[streamIdx]->nextBatch(vector)) {
    VELOX_CHECK_NULL(vector);
    sources_[streamIdx]->enqueue(nullptr, &future);
    return;
  }

  sources_[streamIdx]->enqueue(std::move(vector), &future);
  std::move(future)
      .via(executor_)
      .thenValue([mergeHolder = std::weak_ptr(shared_from_this()),
                  streamIdx](folly::Unit) {
        const auto self = mergeHolder.lock();
        if (self == nullptr) {
          LOG(ERROR)
              << "SpillMerger is destroyed, abandon reading from batch stream";
          return;
        }
        self->readFromSpillFileStream(streamIdx);
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [mergeHolder = std::weak_ptr(shared_from_this()),
           streamIdx](const std::exception& e) {
            LOG(ERROR) << "Stop the " << streamIdx
            << "th batch stream producer for " << e.what();
                        const auto self = mergeHolder.lock();
                        if (self == nullptr) {
                          LOG(ERROR)
                              << "SpillMerger is destroyed, abandon enqueuing end signal.";
                          return;
                        }
                        ContinueFuture future;
                        self->sources_[streamIdx]->enqueue(nullptr, &future);
                      });
}

void AsyncSpillMerger::scheduleAsyncSpillFileStreamReads() {
  VELOX_CHECK_EQ(batchStreams_.size(), sources_.size());
  for (auto i = 0; i < batchStreams_.size(); ++i) {
    executor_->add(
        [&, streamIdx = i]() { readFromSpillFileStream(streamIdx); });
  }
}

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
      4 << 20,
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
  CpuWallTimer timer(readTiming_);
  MicrosecondTimer timer1(&executionTimeUs_);

  if (FLAGS_spill_merger_benchmark_method == 0) {
    LOG(INFO) << "Running SpillMergerBenchmark in async mode";
    const auto asyncSpillMerger = std::make_shared<AsyncSpillMerger>(
      sortingKeys_,
      rowType_,
      inputVectorSize_,
      std::move(spillReadFilesGroups_),
      &spillConfig_,
      spillMergerPool_.get());
    asyncSpillMerger->start();

    std::vector<ContinueFuture> sourceBlockingFutures;
    std::vector<RowVectorPtr> results;
    for (;;) {
      bool atEnd = false;
      const auto output = asyncSpillMerger->getOutput(sourceBlockingFutures, atEnd);
      if (output != nullptr) {
        ++numBatches;
        numRows += output->size();
      }

      if (atEnd) {
        break;
      }

      while (!sourceBlockingFutures.empty()) {
        auto future = std::move(sourceBlockingFutures.back());
        sourceBlockingFutures.pop_back();
        future.wait();
      }
    }

  } else if (FLAGS_spill_merger_benchmark_method == 1) {


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
  LOG(INFO) << "spill wall time: " << succinctNanos(spillTiming_.wallNanos);
  LOG(INFO) << "spill cpu time: " << succinctNanos(spillTiming_.cpuNanos);
  LOG(INFO) << "read wall time: " << succinctNanos(readTiming_.wallNanos);
  LOG(INFO) << "read cpu time: " << succinctNanos(readTiming_.cpuNanos);
  LOG(INFO) << "open file wall time: " << succinctNanos(fileOpenTiming_.wallNanos);
  LOG(INFO) << "open file cpu time: " << succinctNanos(fileOpenTiming_.cpuNanos);

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
  CpuWallTimer timer(spillTiming_);
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
  std::vector<std::vector<std::unique_ptr<SpillReadFile>>> spillReadFilesGroups;
  for (auto i = 0; i < numStreams; ++i) {
    const auto vectors = generateSortedVectors();
    const auto spillFiles = generateSortedSpillFiles(vectors);
    std::vector<std::unique_ptr<SpillReadFile>> spillReadFiles;
    spillReadFiles.reserve(spillFiles.size());

    CpuWallTimer timer(fileOpenTiming_);
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