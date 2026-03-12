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
#include "velox/examples/paimon/PaimonConnector.h"
#include <gtest/gtest.h>
#include "velox/examples/paimon/PaimonDataSource.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::paimon;

class PaimonConnectorTest : public exec::test::HiveConnectorTestBase {
 public:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }
};

TEST_F(PaimonConnectorTest, factoryRegistration) {
  auto factory = std::make_shared<PaimonConnectorFactory>();
  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  auto connector = factory->newConnector("paimon", config);
  facebook::velox::connector::registerConnector(connector);

  auto registeredConnector = facebook::velox::connector::getConnector("paimon");
  EXPECT_NE(registeredConnector, nullptr);
  EXPECT_EQ(registeredConnector->connectorId(), "paimon");
}

TEST_F(PaimonConnectorTest, splitSerialization) {
  std::vector<PaimonDataFileMeta> files;
  files.push_back({"file1.orc", 1024, 0});
  files.push_back({"file2.orc", 2048, 0});

  PaimonConnectorSplit split(
      "test_connector", std::move(files), {{"ds", "2023-01-01"}}, 1);

  EXPECT_EQ(split.files().size(), 2);
  EXPECT_EQ(split.bucketId().value(), 1);
  EXPECT_EQ(split.toString(), "PaimonSplit: 2 files, bucket 1");
}
