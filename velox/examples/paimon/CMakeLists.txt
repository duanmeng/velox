# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

velox_add_library(velox_example_paimon OBJECT PaimonConnector.cpp PaimonDataSource.cpp)

velox_link_libraries(velox_example_paimon PRIVATE velox_hive_connector velox_connector velox_core)

add_executable(velox_example_paimon_demo Demo.cpp)

target_link_libraries(
  velox_example_paimon_demo
  PRIVATE velox_example_paimon velox_exec velox_exec_test_lib velox_vector velox_common_base
)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
