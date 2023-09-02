//
// Created by serdar on 7/5/23.
//

#ifndef ARROW_BACKEND_H
#define ARROW_BACKEND_H

// #include "ccms.h"
#include "schema_info.h"
#include "arrow_schema_info.h"

#include "postgresql_backend.h"

#include <arrow/api.h>
// #include <arrow/status.h>
// #include <arrow/table_builder.h>
// #include <arrow/type.h>
#include <libpq-fe.h>

// #include <iostream>
// #include <map>
// #include <string>
// #include <vector>

namespace ccms_target {

    class ArrowSession {
    private:
        int fetchSize {0};
        int fetchIteration {0};
        int numRowsInIteration {0};
        int numRowsTotal {0};

    public:
        bool schemaRetrieved = false;
        ccms_target::ArrowTable* arrowTable;
        std::unique_ptr<arrow::RecordBatchBuilder> batch_builder;
        void disconnect();
        //PGresult* getResult();
        //PGconn* getConnection();
        arrow::Status setTableMetadata(ccms_source::Table* table);
        arrow::Status appendToBuilder(int colNum, const char * value);
        //int getNumRowsInIteration();
        //int getNumRowsTotal();
        //int getFetchIteration();
    };

}

#endif //ARROW_BACKEND_H