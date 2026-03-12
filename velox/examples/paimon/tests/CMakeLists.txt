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

add_executable(velox_example_paimon_test PaimonConnectorTest.cpp PaimonReadTest.cpp)

# Copy test data to the build directory
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/nation.parquet DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(
  velox_example_paimon_test
  velox_example_paimon
  velox_exec_test_lib
  velox_hive_connector
  GTest::gtest
  GTest::gtest_main
)

add_test(NAME velox_example_paimon_test COMMAND velox_example_paimon_test)
