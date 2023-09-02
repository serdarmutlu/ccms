//
// Created by serdar on 7/5/23.
//

#ifndef ARROW_SCHEMA_INFO_H
#define ARROW_SCHEMA_INFO_H

// #include "ccms.h"
#include "schema_info.h"
#include "postgresql_backend.h"

#include <arrow/api.h>
// #include <arrow/status.h>
// #include <arrow/table_builder.h>
// #include <arrow/type.h>
#include <c.h>
#include <libpq-fe.h>
#include <string>

// #include <iostream>
// #include <map>
// #include <string>
// #include <vector>

namespace ccms_target {

    
    struct ArrowMetadataRow {
        std::string dbhost;
        std::string dbport;
        std::string username;
        std::string password;
        std::string database;
        std::string tablename;
        std::string arrowtablename;
    };

    class ArrowTable {
    public:
        ccms_source::Table sourceTable;
        // std::vector<std::shared_ptr<arrow::Field>> fields {};
        std::shared_ptr<arrow::Schema> schema {};
        std::string name {};
        std::shared_ptr<arrow::Table> table;
        //std::shared_ptr<arrow::RecordBatch> batch {};
        //std::vector<arrow::ArrayBuilder*> builder {};
        ArrowTable() {};
        ArrowTable(std::string name, std::shared_ptr<arrow::Table> table) {
            this->name = name;
            this->table = table;
        };
        ArrowTable(ccms_source::Table* toCopy);
    };

    class ArrowServer {
    private:
        int numberOfTables {0};
        std::map<std::string, ArrowTable> tables; 
        std::vector<ccms_target::ArrowMetadataRow> arrowMetadata;
        void readArrowMetadata();
        void printArrowMetadata();
        arrow::Status populateArrowTables();
        bool writeArrowMetadata();
    public:

        bool addTable(std::string tableName, ArrowTable arrowTable);
        bool deleteTable(std::string tableName);
        int getNumberOfTables();
        ArrowServer() {};
        bool startArrowServer();
        bool shutdownArrowServer(); 
    };


}

#endif //ARROW_SCHEMA_INFO_H