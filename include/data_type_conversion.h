//
// Created by serdar on 6/3/23.
//

#ifndef DATA_TYPE_CONVERSION_H
#define DATA_TYPE_CONVERSION_H

#include "ccms.h"

#include <arrow/type_fwd.h>
#include <string>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

class DataTypeConversion {
public:
    static parquet::Type::type convertPgToParquetType(const std::string& pgType);
    static std::string convertPgToArrowType(const std::string& pgType);
    static parquet::Type::type convertArrowToParquetType(const std::string& arrowType);
    static std::string convertPgOidToDataType(int oid);
    static std::shared_ptr<arrow::DataType> pgOidToArrowType(int pgOid);
};


#endif //DATA_TYPE_CONVERSION_H
