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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/exec/trace/QueryTraceDataReader.h"

namespace facebook::velox::exec {

class QueryTraceScan final : public SourceOperator {
 public:
  QueryTraceScan(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::QueryTraceScanNode>&
          queryTraceScanNode);

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

 private:
  bool finished_{false};
  std::unique_ptr<QueryTraceDataReader> traceReader_;
};

} // namespace facebook::velox::exec