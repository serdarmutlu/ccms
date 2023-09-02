//
// Created by serdar on 6/3/23.
//

#include "include/arrow_backend.h"
#include "include/ccms.h"
#include "include/data_type_conversion.h"
#include "include/postgresql_backend.h"
#include "include/schema_info.h"

#include <arrow/status.h>
#include <arrow/util/logging.h>
#include <chrono>
#include <iostream>
#include <libpq-fe.h>
#include <string>

#include "postgres.h" // Keep it at the bottom of includes
#if PG_VERSION_NUM < 110000
#include "catalog/pg_type.h"
#else
#include "catalog/pg_type_d.h"
#endif



arrow::Status
ccms_target::ArrowSession::setTableMetadata(ccms_source::Table* tableInfo) {
  std::vector<std::shared_ptr<arrow::Field>> fields{};
  std::shared_ptr<arrow::Schema> schema{};

  this->arrowTable = new ccms_target::ArrowTable(tableInfo);
  int numColumns = this->arrowTable->sourceTable.numColumns;

  for (int i{0}; i < numColumns; i++) {
    std::shared_ptr<arrow::DataType> arrowType =
        DataTypeConversion::pgOidToArrowType(
            this->arrowTable->sourceTable.columns.at(i).type);
    fields.push_back(
        arrow::field(this->arrowTable->sourceTable.columns.at(i).name, arrowType));
  }
  schema = arrow::schema(fields);

  ARROW_ASSIGN_OR_RAISE(this->batch_builder,
                        arrow::RecordBatchBuilder::Make(
                            schema, arrow::default_memory_pool(), 100));

  // for (int i{0}; i < batch_builder->num_fields(); i++) {
  //   this->arrowTable->builder.push_back(batch_builder->GetField(i));
  // }
  this->schemaRetrieved = true;

  return arrow::Status::OK();
};

arrow::Status ccms_target::ArrowSession::appendToBuilder(int colNum,
                                                         const char *value) {
  auto batchBuilderField = this->batch_builder->GetField(colNum);
  switch (this->arrowTable->sourceTable.columns.at(colNum).type) {
  case INT2OID: {
    ARROW_RETURN_NOT_OK(
        static_cast<arrow::Int16Builder *>(batchBuilderField)->Append(std::stoi(value)));
    break;
  }
  case INT4OID: {
    // ARROW_RETURN_NOT_OK(
    //     static_cast<arrow::Int32Builder
    //     *>(this->arrowTable->builder.at(colNum))
    //         ->Append(std::stol(value)));
    ARROW_RETURN_NOT_OK(static_cast<arrow::Int32Builder *>(batchBuilderField)
                            ->Append(std::stol(value)));

    break;
  }
  case INT8OID: {
    ARROW_RETURN_NOT_OK(
        static_cast<arrow::Int64Builder *>(batchBuilderField)
            ->Append(std::stoll(value)));
    break;
  }
  case VARCHAROID: {
    ARROW_RETURN_NOT_OK(static_cast<arrow::StringBuilder *>(batchBuilderField)
                            ->Append(value));
    break;
  }
  case FLOAT8OID: {
    ARROW_RETURN_NOT_OK(static_cast<arrow::DoubleBuilder *>(batchBuilderField)
                            ->Append(std::stod(value)));
    break;
  }
  }

  return arrow::Status::OK();
}

// int ccms_target::ArrowSession::getNumRowsInIteration() {
//   return numRowsInIteration;
// };

// int ccms_target::ArrowSession::getNumRowsTotal() { return numRowsTotal; };

// int ccms_target::ArrowSession::getFetchIteration() { return fetchIteration; };

/* bool ArrowSession::getSchemaRetrieved() {
    return schemaRetrieved;
}; */











