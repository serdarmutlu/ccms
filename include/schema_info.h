//
// Created by serdar on 7/7/23.
//

#ifndef SCHEMA_INFO_H
#define SCHEMA_INFO_H

#include "ccms.h"

#include <arrow/type.h>
#include <memory>
#include <string>
#include <vector>

namespace ccms_source {

  class Column {
  public:
    std::string name {};
    unsigned int type {0};
    std::string typeName {};
    unsigned int size {0};
    unsigned int precision {0};
    int scale;
  };

  class Table {
  public:
    DataSource dataSource {Postgresql};
    std::string name {};
    std::vector<Column> columns {};
    std::map<std::string, int> columnPositionsByName;
    unsigned int numColumns {0};
    void addColumn(Column columnInfo);
    // Table() {};
    // Table(DataSource dataSource, std::string tableName);
  };



  class Schema {
  public:
    std::vector<Table> tables{};
    int numTables {0};
  };

}

#endif // SCHEMA_INFO_H