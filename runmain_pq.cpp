//
// Created by serdar on 6/8/23.
//

#include "include/runmain_pq.h"
#include "include/arrow_backend.h"
#include "include/postgresql_backend.h"

#include "include/ccms.h"
#include "include/generic_tools.h"
#include "include/timer.h"

#include <arrow/util/logging.h>
#include <catalog/pg_type_d.h>
#include <iostream>
#include <map>
#include <ostream>

#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
#include <sys/sysinfo.h>
#elif defined(__APPLE__) && defined(__MACH__)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

ccms_target::ArrowSession arrowSession;

class testObj {
  int id;
  std::shared_ptr<arrow::Table> table;
  public:
  testObj(int pid, std::shared_ptr<arrow::Table> ptable) {
    id=pid;
    table=ptable;
  };
};

arrow::Status RunMain_pq::testPgAndArrow(std::string queryStatement,
                                         StatementType statementType,
                                         bool colFirst) {

  PgSession pgSession;
  PGresult *result;

  pgSession.connect("pglnx", "5432", "serdar", "serdar", "serdar");
  std::cout << "Connected via libpq to Postgres" << std::endl << std::endl;

  if (colFirst) {
    GenericTools::PrintStats("Started " +
                             StatementTypeMap[StatementType(statementType)] +
                             " - Col first");
  } else {
    GenericTools::PrintStats("Started " +
                             StatementTypeMap[StatementType(statementType)] +
                             " - Row first");
  }
  /*   arrow::Int32Builder int32builder;
    arrow::StringBuilder stringBuilder;
    arrow::Int32Builder int32builder2;
    arrow::StringBuilder stringBuilder2; */

  auto runQueryStatus = pgSession.query(queryStatement, statementType);
  if (!runQueryStatus)
    return arrow::Status::ExecutionError("Unable to run query");

  while (pgSession.fetch()) {
    // GenericTools::PrintStats("Fetch " +
    // std::to_string(pgSession.getFetchIteration()));

    result = pgSession.getResult();

    auto numCols = pgSession.getTableMetadata()->numColumns;
    auto numRows = pgSession.getNumRowsInIteration();

    // unsigned int colType[numCols];

    // int iteration = pgSession.getFetchIteration();
    // GenericTools::PrintStats("Result retrieved from Postgresql, iteration:" +
    // iteration);

    if (!arrowSession.schemaRetrieved)
      auto status = arrowSession.setTableMetadata(pgSession.getTableMetadata());

    /*     for (int column = 0; column < numCols; column++) {
          colType[column] = pgSession.getSchemaColumnTypes().at(column);
        } */

    if (colFirst) {
      for (uint j = 0; j < numCols; j++) {
        for (int i = 0; i < numRows; i++) {
          const char *value = pgSession.getResultByColRow(j,i);
          //const char *value = PQgetvalue(result, i, j);
          auto status = arrowSession.appendToBuilder(j, value);
        }
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        for (uint j = 0; j < numCols; j++) {
          const char *value = pgSession.getResultByColRow(j,i);
          //const char *value = PQgetvalue(result, i, j);
          auto status = arrowSession.appendToBuilder(j, value);
        }
      }
    }
    // GenericTools::PrintStats("Arrow append " +
    // std::to_string(pgSession.getFetchIteration()));

    PQclear(result);
  }
  // pgSession.disconnect();
  if (colFirst) {
    GenericTools::PrintStats("Ended " +
                             StatementTypeMap[StatementType(statementType)] +
                             " - Col first");
  } else {
    GenericTools::PrintStats("Ended " +
                             StatementTypeMap[StatementType(statementType)] +
                             " - Row first");
  }
  return arrow::Status::OK();
};

arrow::Status RunMain_pq::RunMain() {

  int rows;
  std::cout << "Please spesify set size: ";
  std::cin >> rows;

  /*auto x1 = testPgAndArrow("SELECT * FROM t1 limit " + std::to_string(rows),
  StandardQuery, false); auto x2 = testPgAndArrow("SELECT * FROM t1 limit " +
  std::to_string(rows), StandardQuery, true); auto x3 = testPgAndArrow("SELECT *
  FROM t1 limit " + std::to_string(rows), CursorFetchAll, false); auto x4 =
  testPgAndArrow("SELECT * FROM t1 limit " + std::to_string(rows),
  CursorFetchAll, true); auto x5 = testPgAndArrow("SELECT * FROM t1 limit " +
  std::to_string(rows), CursorFetchPartially, false); auto x6 =
  testPgAndArrow("SELECT * FROM t1 limit " + std::to_string(rows),
  CursorFetchPartially, true); auto x7 = testPgAndArrow("SELECT * FROM t1 limit
  " + std::to_string(rows), SingleRow, false);*/
  auto x8 = testPgAndArrow("SELECT * FROM t1 limit " + std::to_string(rows),
                           SingleRow, true);
  
  
  std::shared_ptr<arrow::RecordBatch> recordBatch;


  ARROW_ASSIGN_OR_RAISE(recordBatch, arrowSession.batch_builder->Flush());

  // Use RecordBatch::ValidateFull() to make sure arrays were correctly
  // constructed.
  DCHECK_OK(recordBatch->ValidateFull());
  std::shared_ptr<arrow::Table> table;
  ARROW_ASSIGN_OR_RAISE(table, arrow::Table::FromRecordBatches({recordBatch}));

  auto c1 = table->column(0);
  auto c2 = table->column(1);
  std::cout << c1->length() << " " << c1->ToString() << std::endl;
  std::cout << c2->length() << " " << c2->ToString() << std::endl;


  std::cout << "Size of arrow table:" << sizeof(table) << std::endl;

  GenericTools::PrintStats("Finished...");

  return arrow::Status::OK();
};
