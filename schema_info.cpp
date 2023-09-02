#include "include/schema_info.h"

#include <string>
#include <vector>

// ccms_source::Table::Table(DataSource dataSource, std::string tableName) {
//   this->dataSource = dataSource;
//   this->name = tableName;
// };

void ccms_source::Table::addColumn(ccms_source::Column columnInfo) {
  columns.push_back(columnInfo);
};