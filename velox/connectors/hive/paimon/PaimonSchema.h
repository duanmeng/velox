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

#include <optional>
#include <unordered_map>

#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::hive::paimon {

/// Paimon logical attributes parallel to a Velox Type. Only ROW children have
/// DataField IDs; ARRAY elements and MAP keys/values do not acquire synthetic
/// IDs here. Logical roots and parameters retain distinctions such as
/// CHAR/VARCHAR and timestamp precision that a Velox Type alone cannot express.
struct PaimonTypeMetadata {
  std::string logicalType;
  bool nullable{true};
  std::optional<int32_t> fieldId{};
  /// Length for character/binary types, precision and scale for DECIMAL, or
  /// precision for timestamps. Empty for unparameterized types.
  std::vector<int32_t> parameters{};
  std::vector<PaimonTypeMetadata> children{};

  bool operator==(const PaimonTypeMetadata& other) const;

  /// Checks the entire subtree, including unique field IDs and ROW names.
  void validate(const TypePtr& type, bool isField) const;

  /// Resolves a required subfield rooted at 'name' to its DataField ID path.
  /// Container subscripts are validated but do not add synthetic field IDs.
  std::vector<int32_t> fieldIdPath(
      std::string_view name,
      const TypePtr& type,
      const common::Subfield& subfield) const;

  folly::dynamic serialize() const;
  static PaimonTypeMetadata create(const folly::dynamic& obj);
};

/// Fixed, normalized table schema supplied by the caller, not a parser for
/// Paimon's schema JSON. 'version' is the source schema format version (1-3),
/// not the schema ID or handle wire version. Historical option defaults are
/// applied only to that schema and never overwrite explicit options.
class PaimonSchema {
 public:
  PaimonSchema(
      int64_t schemaId,
      int32_t version,
      RowTypePtr rowType,
      PaimonTypeMetadata metadata,
      std::vector<int32_t> partitionKeys = {},
      std::vector<int32_t> primaryKeys = {},
      std::unordered_map<std::string, std::string> options = {});

  int64_t schemaId() const {
    return schemaId_;
  }
  int32_t version() const {
    return version_;
  }
  const RowTypePtr& rowType() const {
    return rowType_;
  }
  const PaimonTypeMetadata& metadata() const {
    return metadata_;
  }
  const std::vector<int32_t>& partitionKeys() const {
    return partitionKeys_;
  }
  const std::vector<int32_t>& primaryKeys() const {
    return primaryKeys_;
  }
  const std::unordered_map<std::string, std::string>& options() const {
    return options_;
  }

  /// Child indices in this schema's type tree, not field IDs or output
  /// channels.
  const std::vector<column_index_t>& fieldIndexPath(int32_t fieldId) const;
  TypePtr fieldType(int32_t fieldId) const;
  const PaimonTypeMetadata& fieldMetadata(int32_t fieldId) const;
  std::vector<int32_t> fieldIdPath(const common::Subfield& subfield) const;

  folly::dynamic serialize() const;
  static std::shared_ptr<const PaimonSchema> create(const folly::dynamic& obj);

 private:
  const int64_t schemaId_;
  const int32_t version_;
  const RowTypePtr rowType_;
  const PaimonTypeMetadata metadata_;
  const std::vector<int32_t> partitionKeys_;
  const std::vector<int32_t> primaryKeys_;
  std::unordered_map<std::string, std::string> options_;
  std::unordered_map<int32_t, std::vector<column_index_t>> fieldIndexPaths_;
};

using PaimonSchemaPtr = std::shared_ptr<const PaimonSchema>;

/// Lossless path encoding for the Paimon handle wire. Display strings cannot
/// distinguish a literal field name containing '.' from nested field access.
folly::dynamic serializeSubfield(const common::Subfield& subfield);

common::Subfield deserializeSubfield(const folly::dynamic& obj);

} // namespace facebook::velox::connector::hive::paimon
