
#include "include/arrow_schema_info.h"
#include "include/arrow_backend.h"
#include "include/schema_info.h"
#include "include/generic_tools.h"

#include <arrow/status.h>
#include <arrow/util/logging.h>
#include <iterator>
#include <libpq-fe.h>
#include <vector>

// ccms_target::ArrowMetadataRow::ArrowMetadataRow() {

// };

ccms_target::ArrowTable::ArrowTable(ccms_source::Table* toCopy) {
  this->sourceTable.dataSource = toCopy->dataSource;
  this->sourceTable.name = toCopy->name;
  this->sourceTable.columns = toCopy->columns;
  this->sourceTable.numColumns = toCopy->numColumns;
};

int ccms_target::ArrowServer::getNumberOfTables() {
  return this->tables.size();
};

bool ccms_target::ArrowServer::addTable(std::string tableName,
                                        ArrowTable arrowTable) {
  this->tables.insert_or_assign(tableName, arrowTable);
  return true;
};

bool ccms_target::ArrowServer::deleteTable(std::string tableName) {
  this->tables.erase(tableName);
  return true;
};

bool ccms_target::ArrowServer::startArrowServer() {
  readArrowMetadata();
  printArrowMetadata();
  auto status = populateArrowTables();
  return true;
};

bool ccms_target::ArrowServer::shutdownArrowServer() {

  while (this->tables.size()>0)
  {
    std::map<std::string, ArrowTable>::iterator it = this->tables.begin();

    GenericTools::PrintStats("Before reset");
    it->second.table.reset();
    GenericTools::PrintStats("After reset");
    this->deleteTable(it->first);
    GenericTools::PrintStats("After delete");
  }

  return true;
};

void ccms_target::ArrowServer::readArrowMetadata() {
  PgSession metadataSession;
  PGresult* metadataResult;
  std::string queryStatement{};
  StatementType statementType{};

  metadataSession.connect("pglnx", "5432", "serdar", "serdar", "metadata");

  queryStatement = "select * from arrowtable";
  statementType = StandardQuery;

  auto runQueryStatus = metadataSession.query(queryStatement, statementType);
  if (!runQueryStatus)
    std::cerr << "Unable to execute";

  while (metadataSession.fetch()) {
    // GenericTools::PrintStats("Fetch " +
    // std::to_string(pgSession.getFetchIteration()));

    metadataResult = metadataSession.getResult();

    // auto numCols = metadataSession.getTableMetadata().numColumns;
    auto numRows = metadataSession.getNumRowsInIteration();

    // if (!metadataSession.getTableMetadataRetrieved())
    //    metadataSession.setTableMetadata();

    ArrowMetadataRow arrowMetadataRow = ArrowMetadataRow();
    for (int i = 0; i < numRows; i++) {
        arrowMetadataRow.dbhost = metadataSession.getResultByColnameRow("dbhost", i);
        arrowMetadataRow.dbport = metadataSession.getResultByColnameRow("dbport", i);
        arrowMetadataRow.username =
            metadataSession.getResultByColnameRow("username", i);
        arrowMetadataRow.password =
            metadataSession.getResultByColnameRow("password", i);
        arrowMetadataRow.database =
            metadataSession.getResultByColnameRow("database", i);
        arrowMetadataRow.tablename =
            metadataSession.getResultByColnameRow("tablename", i);
        arrowMetadataRow.arrowtablename =
            metadataSession.getResultByColnameRow("arrowtablename", i);
        this->arrowMetadata.push_back(arrowMetadataRow);

    }
    PQclear(metadataResult);
  };

};


void ccms_target::ArrowServer::printArrowMetadata() {
  std::vector<ccms_target::ArrowMetadataRow> arrowMetadata{};
  ccms_target::ArrowMetadataRow arrowMetadataRow;

  for (unsigned long i=0; i<this->arrowMetadata.size(); i++) {
    arrowMetadata.push_back(this->arrowMetadata.at(i));
  }

    uint numRows = arrowMetadata.size();

    for (uint i = 0; i < numRows; i++) {
      arrowMetadataRow = arrowMetadata.at(i);
      std::string hostName = arrowMetadataRow.dbhost;
      std::string portNum = arrowMetadataRow.dbport;
      std::string username = arrowMetadataRow.username;
      std::string password = arrowMetadataRow.password;
      std::string database = arrowMetadataRow.database;
      std::string arrowTableName = arrowMetadataRow.arrowtablename;
      std::string tableString(arrowMetadataRow.tablename);

      std::cout << hostName << " " << portNum << " " << username << " " << password << " " << database << " " << arrowTableName << " " << tableString  << std::endl;
    };
};

arrow::Status ccms_target::ArrowServer::populateArrowTables() {

  std::string queryStatement{};
  std::string arrowTableName;
  //StatementType statementType{};
  ccms_target::ArrowMetadataRow arrowMetadataRow = ccms_target::ArrowMetadataRow();

  // std::vector<ccms_target::ArrowMetadataRow> arrowMetadata{};
  
  // for (unsigned long i=0; i<this->arrowMetadata.size(); i++) {
  //   arrowMetadataRow = this->arrowMetadata.at(i);
  //   arrowMetadata.push_back(arrowMetadataRow);
  // }


  uint numRows = this->arrowMetadata.size();

  for (uint i = 0; i < numRows; i++) {
    PgSession pgSession;
    PGresult* pgResult;
    arrowMetadataRow = this->arrowMetadata.at(i);
    std::string hostName = arrowMetadataRow.dbhost;
    std::string portNum = arrowMetadataRow.dbport;
    std::string username = arrowMetadataRow.username;
    std::string password = arrowMetadataRow.password;
    std::string database = arrowMetadataRow.database;
    
    pgSession.connect(hostName, portNum, username, password, database);
    //pgSession.connect("127.0.0.1", "5432", "serdar", "serdar", "serdar");

    arrowTableName = arrowMetadataRow.arrowtablename;
    std::string tableString(arrowMetadataRow.tablename);
    queryStatement = "select * from " + tableString;


    auto runQueryStatus = pgSession.query(queryStatement);
    if (!runQueryStatus)
      return arrow::Status::KeyError("Unable to execute");

    unsigned int numColsPgTable = pgSession.getTableMetadata()->numColumns;
    ccms_target::ArrowSession arrowSession;
    std::vector<std::shared_ptr<arrow::Array>> chunks[numColsPgTable];

    while (pgSession.fetch()) {
      pgResult = pgSession.getResult();

      std::shared_ptr<arrow::Array> array[numColsPgTable];

      auto numRowsPgTable = pgSession.getNumRowsInIteration();

      if (!arrowSession.schemaRetrieved)
        auto status =
            arrowSession.setTableMetadata(pgSession.getTableMetadata());

      for (uint j = 0; j < numColsPgTable; j++) {
        for (int i = 0; i < numRowsPgTable; i++) {
          const char *value = pgSession.getResultByColRow(j, i);
          // const char *value = PQgetvalue(result, i, j);
          auto status = arrowSession.appendToBuilder(j, value);
        }
        auto batchBuilderField = arrowSession.batch_builder->GetField(j);
            if (!batchBuilderField->Finish(&array[j]).ok()) {
                // ... do something on array building failure
            }

            chunks[j].push_back(std::move(array[j]));

            batchBuilderField->Reset();
      }
      std::cout << "A fetch set finished" << std::endl;
    }
    PQclear(pgResult);
    
    std::vector<std::shared_ptr<arrow::ChunkedArray>> chunkedArrayColumns;
    
    for (uint i=0; i<numColsPgTable; i++) {
      chunkedArrayColumns.push_back(std::make_shared<arrow::ChunkedArray>(std::move(chunks[i])));
    };
 
    std::shared_ptr<arrow::Table> arrowTable = arrow::Table::Make(arrowSession.arrowTable->schema, chunkedArrayColumns, -1);

    // std::shared_ptr<arrow::RecordBatch> recordBatch;



    // ARROW_ASSIGN_OR_RAISE(recordBatch, arrowSession.batch_builder->Flush());

    // // Use RecordBatch::ValidateFull() to make sure arrays were correctly
    // // constructed.
    // DCHECK_OK(recordBatch->ValidateFull());
    // std::shared_ptr<arrow::Table> table;
    // ARROW_ASSIGN_OR_RAISE(table, arrow::Table::FromRecordBatches({recordBatch}));


    ArrowTable* arrowTableDef = new ArrowTable(arrowTableName, arrowTable);
    this->addTable(arrowTableName, *arrowTableDef);

    this->numberOfTables++;
  }
  return arrow::Status::OK();
};