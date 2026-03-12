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
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/examples/paimon/PaimonConnector.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::paimon;

// Simple demo to show how to register PaimonConnector and use it.
// Note: This is a structural demo, actual reading requires real Paimon data
// files.
int main() {
  // 1. Init Velox
  memory::MemoryManager::initialize({});

  // 2. Register Paimon Connector Factory
  auto factory = std::make_shared<PaimonConnectorFactory>();
  // ConnectorFactory registration is not exposed in Connector.h, so we create
  // connector directly

  // 3. Create a Paimon Connector instance
  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {"hive.s3.use-instance-credentials", "true"}});

  auto connector = factory->newConnector("paimon_test_connector", config);
  facebook::velox::connector::registerConnector(connector);

  std::cout << "Successfully created Paimon Connector: "
            << connector->connectorId() << std::endl;

  return 0;
}
