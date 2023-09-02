//
// Created by serdar on 6/3/23.
//

#ifndef POSTGRESQL_BACKEND_H
#define POSTGRESQL_BACKEND_H

#include "libpq-fe.h"
#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "ccms.h"
#include "schema_info.h"


class PgSession {
private:
    PGconn* conn;
    PGresult* result;
    std::string statement;
    std::string queryStatement;
    std::string cursorStatement;
    StatementType statementType;
    //FetchType fetchType;
    int fetchSize {0};
    int fetchIteration {0};
    std::string cursorName;
    int numRowsInIteration {0};
    int numRowsTotal {0};

    bool createCursor();
    bool closeCursor();
    bool exec(); // Called privately by query, insert, update or delete
public:
    ccms_source::Table tableMetadata;
    void connect(std::string hostName,
                         std::string portNum,
                         std::string username,
                         std::string password,
                         std::string database);
    void disconnect();
    bool query(std::string statement, StatementType statementType=CursorFetchPartially, int fetchSize=500000);
    bool insert(std::string statement);
    bool update(std::string statement);
    bool remove(std::string statement); // Can't use keyword delete, so using remove instead
    bool tableMetadataRetrieved = false;
    
    //bool fetchFromCursor();
    bool fetch();
    PGresult* getResult();
    const char* getResultByColRow(int Col, int Row);
    const char* getResultByColnameRow(std::string Colname, int Row);
    PGconn* getConnection();
    ccms_source::Table* getTableMetadata();
    void setTableMetadata();
    int getNumRowsInIteration();
    int getNumRowsTotal();
    int getFetchIteration();
    //bool getTableMetadataRetrieved();
    PgSession();
    ~PgSession();
    PgSession& test();
};



#endif //POSTGRESQL_BACKEND_H