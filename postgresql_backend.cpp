//
// Created by serdar on 6/3/23.
//

#include "include/postgresql_backend.h"
#include "include/data_type_conversion.h"
#include "include/schema_info.h"
#include "postgres.h"
#include <arrow/type_fwd.h>
#include <chrono>
#include <iostream>
#include <libpq-fe.h>
#include <string>

#if PG_VERSION_NUM < 110000
#include "catalog/pg_type.h"
#else
#include "catalog/pg_type_d.h"
#endif

/* static void
exit_nicely(PGconn *conn)
{
  PQfinish(conn);
  exit(1);
} */

/* static void
setting_failed(PGconn *conn)
{
  fprintf(stderr, "Unable to enter single row mode: %s\n",
          PQerrorMessage(conn));
  exit_nicely(conn);
} */

void PgSession::connect(std::string hostName, std::string portNum,
                        std::string username, std::string password,
                        std::string database) {
  // PGconn	   *conn;
  const char *keys[20];
  const char *values[20];
  int index{0};

  if (!hostName.empty()) {
    keys[index] = "host";
    values[index++] = hostName.c_str();
  }

  if (!portNum.empty()) {
    keys[index] = "port";
    values[index++] = portNum.c_str();
  }

  if (!username.empty()) {
    keys[index] = "user";
    values[index++] = username.c_str();
  }

  if (!password.empty()) {
    keys[index] = "password";
    values[index++] = password.c_str();
  }

  if (!database.empty()) {
    keys[index] = "dbname";
    values[index++] = database.c_str();
  }

  // keys[index] = "application_name";
  // values[index++] = "pg_arrow";

  /* terminal */
  keys[index] = NULL;
  values[index] = NULL;

  conn = PQconnectdbParams(keys, values, 0);
  if (PQstatus(this->conn) != CONNECTION_OK) {
    std::cout << "Failed on PostgreSQL connection: "
              << PQerrorMessage(this->conn);
    disconnect();
  }
};

bool PgSession::query(std::string statement, StatementType statementType,
                      int fetchSize) {
  this->statement = statement;
  this->queryStatement = statement;
  this->statementType = statementType;
  this->fetchSize = fetchSize;
  this->fetchIteration = 0;
  this->setTableMetadata();
  return this->exec();
};

bool PgSession::exec() {
  switch (this->statementType) {
    case StandardQuery: {
      break;
    }
    case SingleRow: {
      PQsendQuery(this->getConnection(), this->queryStatement.c_str());
      PQsetSingleRowMode(this->getConnection());
      break;
    }
    case CursorFetchAll: {
      if (!createCursor())
        return false;
      this->cursorStatement = "FETCH ALL in " + this->cursorName;
      break;
    }
    case CursorFetchPartially: {
      if (!createCursor())
        return false;
      this->cursorStatement = "FETCH FORWARD " + std::to_string(this->fetchSize) +
                              " FROM " + this->cursorName;
      break;
    }
    case Insert:
    case Update:
    case Delete: {
      break;
    }
  }

  return true;
};

bool PgSession::createCursor() {
  std::string cursorStatement = "BEGIN";
  this->result = PQexec(this->conn, cursorStatement.data());
  if (PQresultStatus(this->result) != PGRES_COMMAND_OK) {
    std::cout << "Failed on PostgreSQL cursor begin: %s",
        PQerrorMessage(this->conn);
    PQclear(this->result);
  }
  // We will get current time as a number since epoch(01.01.1970).
  // We can have many parallel createCursor statements
  // So, current time will be used to create a cursor with a unique name
  // For ex: "DECLARE cs213231294832414 CURSOR FOR select * from employees"

  auto time = std::chrono::system_clock::now(); // get the current time
  auto since_epoch = time.time_since_epoch();   // get the duration since epoch
  auto nanosec =
      std::chrono::duration_cast<std::chrono::nanoseconds>(since_epoch);
  cursorName = "cs" + std::to_string(nanosec.count());

  cursorStatement =
      "DECLARE " + cursorName + " CURSOR FOR " + this->queryStatement;
  result = PQexec(this->conn, cursorStatement.data());
  if (PQresultStatus(this->result) != PGRES_COMMAND_OK) {
    std::cout << "Failed on PostgreSQL cursor: %s", PQerrorMessage(this->conn);
    PQclear(this->result);
    return false;
  }
  // this->fetchType = fetchType;
  // this->fetchIteration = 0;
  return true;
};

bool PgSession::closeCursor() {
  std::string cursorStatement = "CLOSE " + this->cursorName;
  PQexec(this->getConnection(), cursorStatement.c_str());
  cursorStatement = "COMMIT";
  PQexec(this->getConnection(), cursorStatement.c_str());
  return true;
};

bool PgSession::fetch() {

  // std::string statement;

  // We can fetch all record to client with "FETCH ALL", or  with "FETCH
  // FORWARD"(ex:1000 records per call) Fetching all will take less time but
  // will stress memory at the client side Fetching partially will take slightly
  // more time but will use less memory at the client side

  switch (this->statementType) {
    case StandardQuery: {
      if (this->fetchIteration > 0)
        return false;
      this->result = PQexec(this->conn, this->queryStatement.c_str());
      break;
    }
    case SingleRow: {
      this->result = PQgetResult(this->conn);
      break;
    }
    case CursorFetchAll:
    case CursorFetchPartially: {
      this->result = PQexec(this->conn, this->cursorStatement.c_str());
      break;
    }
    case Insert:
    case Update:
    case Delete: {
      break;
    }
  }

  // if (!this->tableMetadataRetrieved)
  //   this->setTableMetadata();

  this->fetchIteration++;
  int rowCount = PQntuples(this->result);
  this->numRowsInIteration = rowCount;
  this->numRowsTotal += rowCount;

  switch (PQresultStatus(this->result)) {
    case PGRES_SINGLE_TUPLE: {
      break;
    }
    case PGRES_TUPLES_OK: {
      if (rowCount == 0) {
        if ((this->statementType == CursorFetchAll) ||
            (this->statementType == CursorFetchPartially)) {
          closeCursor();
        }
        return false;
      }
      break;
    }
    default: {
      break;
    }
  }
  /*if ((this->fetchIteration%10)==0)
      std::cout << "Iteration:" << this->fetchIteration << std::endl; */

  return true;
};

void PgSession::setTableMetadata() {

  PGresult* metadataResult = PQexec(this->conn, ("select * from (" +this->queryStatement+") as dummy limit 0").c_str());

  // Extract data from the result
  int numColumns = PQnfields(metadataResult);
  this->tableMetadata.numColumns = numColumns;

  for (int i = 0; i < numColumns; i++) {
    const char *name = PQfname(metadataResult, i);
    const unsigned int type = PQftype(metadataResult, i);
    const unsigned int mod = PQfmod(metadataResult, i);
    const unsigned int size = PQfsize(metadataResult, i);
    auto precision = ((mod - 4) >> 16) & 0xffff;
    int scale = (mod - 4) & 0xffff;

    ccms_source::Column column;
    column.name = name;
    column.type = type;
    const std::string typeName =
        DataTypeConversion::convertPgOidToDataType(type);
    column.typeName = typeName;
    column.size = size;
    column.precision = precision;
    if (type == NUMERICOID) /* Defined in pg_type_d.h*/
      scale = (scale > 1000) ? scale - 2048 : scale;
    column.scale = scale;

    this->tableMetadata.addColumn(column);
    this->tableMetadata.columnPositionsByName.insert({name, i});

    std::cout << "Column name:" << name << " type:" << type
              << " typeName:" << typeName << " mod:" << mod << " size:" << size
              << " precision:" << precision << " scale:" << scale << std::endl;
  }

  this->tableMetadataRetrieved = true;
}

void PgSession::disconnect() {
  // PQclear(result);
  PQfinish(this->conn);
  // exit(1);
};

PGresult *PgSession::getResult() { return this->result; };

const char* PgSession::getResultByColRow(int Col, int Row) {
  const char *value = PQgetvalue(this->result, Row, Col);
  return value;
};

const char* PgSession::getResultByColnameRow(std::string Colname, int Row) {
  const char *value = PQgetvalue(this->result, Row, this->getTableMetadata()->columnPositionsByName.at(Colname));
  return value;
};


PGconn *PgSession::getConnection() { return this->conn; };

int PgSession::getNumRowsInIteration() { return this->numRowsInIteration; };

int PgSession::getNumRowsTotal() { return this->numRowsTotal; };

int PgSession::getFetchIteration() { return this->fetchIteration; };

/* bool PgSession::getTableMetadataRetrieved() {
  return this->tableMetadataRetrieved;
}; */

ccms_source::Table* PgSession::getTableMetadata() {
  return &this->tableMetadata;
};

PgSession::PgSession(){

};

PgSession::~PgSession() {
  // PQclear(result);
  PQfinish(this->conn);
  std::cout << "-PgSession destroyed" << std::endl << std::endl;
};

PgSession &PgSession::test() {
  int i = 9;
  std::cout << i;
  return *this;
};