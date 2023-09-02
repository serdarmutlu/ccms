//
// Created by serdar on 6/3/23.
//

#include "include/data_type_conversion.h"

#include <arrow/type_fwd.h>
#include <catalog/pg_type_d.h>
#include <iostream>
#include <unordered_map>
/*
#include <arrow/api.h>
#include <libpq-fe.h>
*/
#include <iostream>
#include <unordered_map>
#include <arrow/api.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

std::shared_ptr<arrow::DataType> DataTypeConversion::pgOidToArrowType(int pgOid){

    std::shared_ptr<arrow::DataType> arrowType;

    switch(pgOid) {
        case INT2OID: {
            arrowType = arrow::int16();
            break;
        }
        case INT4OID: {
            arrowType = arrow::int32();
            break;
        }
        case INT8OID: {
            arrowType = arrow::int64();
            break;
        }
        case FLOAT4OID: {
            arrowType = arrow::float32();
            break;
        }
        case FLOAT8OID: {
            arrowType = arrow::float64();
            break;
        }
        case VARCHAROID: {
            arrowType = arrow::utf8();
            break;
        }

        
    }
    
    return arrowType;

}

std::unordered_map<std::string, std::string> pgToArrowMapping2 = {
        {"bool", "bool"},
        {"bytea", "binary"},
        {"int2", "int16"},
        {"int4", "int32"},
        {"int8", "int64"},
        {"real", "float"},
        {"double precision", "double"},
        {"numeric", "decimal"},
        {"date", "date32"},
        {"time", "time32[s]"},
        {"timestamp", "timestamp[ns]"},
        {"interval", "duration[s]"},
        {"char", "utf8"},
        {"varchar", "utf8"}
};

std::string DataTypeConversion::convertPgToArrowType(const std::string& pgType) {
    auto it = pgToArrowMapping2.find(pgType);
    if (it != pgToArrowMapping2.end()) {
        return it->second;
    } else {
        return "unknown";
    }
}

std::unordered_map<std::string, parquet::Type::type> arrowToParquetMapping = {
        {"bool", parquet::Type::BOOLEAN},
        {"binary", parquet::Type::BYTE_ARRAY},
        {"int8", parquet::Type::INT64},
        {"int16", parquet::Type::INT32},
        {"int32", parquet::Type::INT32},
        {"int64", parquet::Type::INT64},
        {"float", parquet::Type::FLOAT},
        {"double", parquet::Type::DOUBLE},
        {"decimal", parquet::Type::BYTE_ARRAY},
        {"date32", parquet::Type::INT32},
        {"time32[s]", parquet::Type::INT32},
        {"timestamp[ns]", parquet::Type::INT64},
        {"duration[s]", parquet::Type::INT64},
        {"utf8", parquet::Type::BYTE_ARRAY}
};

std::unordered_map<std::string, parquet::Type::type> pgToParquetMapping = {
        {"bool", parquet::Type::BOOLEAN},
        {"bytea", parquet::Type::BYTE_ARRAY},
        {"int2", parquet::Type::INT32},
        {"int4", parquet::Type::INT32},
        {"int8", parquet::Type::INT64},
        {"real", parquet::Type::FLOAT},
        {"double precision", parquet::Type::DOUBLE},
        {"numeric", parquet::Type::BYTE_ARRAY},
        {"date", parquet::Type::INT32},
        {"time", parquet::Type::INT32},
        {"timestamp", parquet::Type::INT64},
        {"interval", parquet::Type::INT64},
        {"char", parquet::Type::BYTE_ARRAY},
        {"varchar", parquet::Type::BYTE_ARRAY}
};


std::map<int, std::string> pgOidToString = {
{16,"boolean"},
{17,"bytea"},
{18,"char"},
{19,"name"},
{20,"bigint"},
{21,"smallint"},
{23,"integer"},
{24,"regproc"},
{25,"text"},
{26,"oid"},
{27,"tid"},
{28,"xid"},
{29,"cid"},
{30,"oidvector"},
{32,"pg_ddl_command"},
{71,"pg_type"},
{81,"pg_attribute"},
{83,"pg_proc"},
{114,"json"},
{142,"xml"},
{194,"pg_node_tree"},
{195,"pg_ndistinct"},
{196,"pg_dependencies"},
{197,"pg_class"},
{198,"pg_attribute_encoding"},
{199,"pg_rewrite"},
{210,"pg_statistic"},
{325,"pg_trigger"},
{600,"point"},
{601,"lseg"},
{602,"path"},
{603,"box"},
{604,"polygon"},
{628,"line"},
{629,"line_segment"},
{650,"cidr"},
{651,"inet"},
{700,"real"},
{701,"double precision"},
{702,"abstime"},
{703,"reltime"},
{704,"tinterval"},
{718,"circle"},
{719,"money"},
{720,"macaddr"},
{721,"inet"},
{790,"macaddr8"},
{829,"anyarray"},
{869,"anyelement"},
{1000,"tsvector"},
{1001,"tsquery"},
{1005,"uuid"},
{1007,"txid_snapshot"},
{1028,"pg_lsn"},
{1042,"bpchar"},
{1043,"varchar"},
{1082,"date"},
{1083,"time"},
{1114,"timestamp"},
{1184,"timestamptz"},
{1186,"interval"},
{1266,"time"},
{1560,"bit"},
{1562,"varbit"},
{1700,"numeric"},
{1790,"refcursor"},
{2201,"cstring"},
{2202,"any"},
{2249,"record"},
{2275,"trigger"},
{2276,"event_trigger"},
{2277,"void"},
{2950,"uuid"},
{2970,"txid_snapshot"},
{3115,"fdw_handler"},
{3257,"anyenum"},
{3310,"tsm_handler"},
{3351,"anyrange"},
{3734,"event_trigger"},
{3769,"tsvector"},
{3770,"tsquery"},
{3802,"gtsvector"},
{3904,"regnamespace"},
{3905,"regrole"},
{3906,"regcollation"},
{3907,"regconfig"},
{3908,"regdictionary"},
{3910,"jsonb"},
{3926,"int4range"},
{3927,"numrange"},
{3928,"tsrange"},
{3929,"tstzrange"},
{3930,"daterange"},
{4089,"regoper"},
{4090,"regoperator"},
{4096,"regclass"},
{4749,"regprocedure"},
{4812,"regtype"},
{5339,"regprocedure"},
{5340,"regoper"},
{5341,"regoperator"},
{6027,"regcollation"},
{7758,"regnamespace"},
{8291,"regconfig"},
{9095,"regdictionary"}
};

parquet::Type::type DataTypeConversion::convertPgToParquetType(const std::string& pgType) {
    auto it = pgToParquetMapping.find(pgType);
    if (it != pgToParquetMapping.end()) {
        return it->second;
    } else {
        return parquet::Type::UNDEFINED;
    }
}



parquet::Type::type DataTypeConversion::convertArrowToParquetType(const std::string& arrowType) {
    auto it = arrowToParquetMapping.find(arrowType);
    if (it != arrowToParquetMapping.end()) {
        return it->second;
    } else {
        return parquet::Type::UNDEFINED;
    }
}


/*

int main() {
    // Simulating a PostgreSQL data type
    std::string postgresType = "varchar";

    // Convert PostgreSQL data type to Arrow data type
    std::string arrowType = getArrowDataType(postgresType);

    // Print the result
    std::cout << "PostgreSQL Data Type: " << postgresType << std::endl;
    std::cout << "Arrow Data Type: " << arrowType << std::endl;

    return 0;
}
 */

 std::string DataTypeConversion::convertPgOidToDataType(int oid) {
    auto it = pgOidToString.find(oid);
    if (it != pgOidToString.end()) {
        return it->second;
    } else {
        return "unknown";
    }
}






/*
Some chatgpt formatted outputs, later noticed 701 is wrong defined :(
{16,"int2oid","smallint"},
{17,"int2vectoroid","smallint[]"},
{18,"int4arrayoid","integer[]"},
{19,"textarrayoid","text[]"},
{20,"int8oid","bigint"},
{21,"float4oid","real"},
{22,"float8oid","double precision"},
{23,"int4oid","integer"},
{24,"regprocoid","regproc"},
{25,"textoid","text"},
{26,"oidoid","oid"},
{27,"tidoid","tid"},
{28,"xidoid","xid"},
{29,"cidoid","cid"},
{30,"oidvectoroid","oidvector"},
{71,"pg_typeoid","pg_type"},
{81,"pg_attributeoid","pg_attribute"},
{83,"pg_procoid","pg_proc"},
{114,"jsonboid","jsonb"},
{118,"xmloid","xml"},
{700,"float8oid","double precision"},
{701,"unknownoid","unknown"},
{1042,"bpcharoid","character"},
{1043,"varcharoid","character varying"},
{1082,"dateoid","date"},
{1083,"timeoid","time without time zone"},
{1114,"timestampoid","timestamp without time zone"},
{1184,"timestamptzoid","timestamp with time zone"},
{1186,"intervaloid","interval"},
{1700,"numericoid","numeric"},
{2278,"voidoid","void"},
{2950,"uuidoid","uuid"},
{3802,"jsonoid","json"},
{3904,"int4rangeoid","int4range"},
{3905,"numrangeoid","numrange"},
{3906,"tsrangeoid","tsrange"},
{3907,"tstzrangeoid","tstzrange"},
{3908,"daterangeoid","daterange"},
{3912,"int8rangeoid","int8range"},
{4089,"regprocedureoid","regprocedure"},
{4096,"regoperoid","regoper"},
{4097,"regoperatoroid","regoperator"},
{4098,"regclassoid","regclass"},
{4099,"regtypeoid","regtype"}

{16,"int2oid"},
{17,"int2vectoroid"},
{18,"int4arrayoid"},
{19,"textarrayoid"},
{20,"int8oid"},
{21,"float4oid"},
{22,"float8oid"},
{23,"int4oid"},
{24,"regprocoid"},
{25,"textoid"},
{26,"oidoid"},
{27,"tidoid"},
{28,"xidoid"},
{29,"cidoid"},
{30,"oidvectoroid"},
{71,"pg_typeoid"},
{81,"pg_attributeoid"},
{83,"pg_procoid"},
{114,"jsonboid"},
{118,"xmloid"},
{700,"float8oid"},
{701,"unknownoid"},
{1042,"bpcharoid"},
{1043,"varcharoid"},
{1082,"dateoid"},
{1083,"timeoid"},
{1114,"timestampoid"},
{1184,"timestamptzoid"},
{1186,"intervaloid"},
{1700,"numericoid"},
{2278,"voidoid"},
{2950,"uuidoid"},
{3802,"jsonoid"},
{3904,"int4rangeoid"},
{3905,"numrangeoid"},
{3906,"tsrangeoid"},
{3907,"tstzrangeoid"},
{3908,"daterangeoid"},
{3912,"int8rangeoid"},
{4089,"regprocedureoid"},
{4096,"regoperoid"},
{4097,"regoperatoroid"},
{4098,"regclassoid"},
{4099,"regtypeoid"}


{16,"smallint"},
{17,"smallint[]"},
{18,"integer[]"},
{19,"text[]"},
{20,"bigint"},
{21,"real"},
{22,"double precision"},
{23,"integer"},
{24,"regproc"},
{25,"text"},
{26,"oid"},
{27,"tid"},
{28,"xid"},
{29,"cid"},
{30,"oidvector"},
{71,"pg_type"},
{81,"pg_attribute"},
{83,"pg_proc"},
{114,"jsonb"},
{118,"xml"},
{700,"double precision"},
{701,"unknown"},
{1042,"character"},
{1043,"character varying"},
{1082,"date"},
{1083,"time without time zone"},
{1114,"timestamp without time zone"},
{1184,"timestamp with time zone"},
{1186,"interval"},
{1700,"numeric"},
{2278,"void"},
{2950,"uuid"},
{3802,"json"},
{3904,"int4range"},
{3905,"numrange"},
{3906,"tsrange"},
{3907,"tstzrange"},
{3908,"daterange"},
{3912,"int8range"},
{4089,"regprocedure"},
{4096,"regoper"},
{4097,"regoperator"},
{4098,"regclass"},
{4099,"regtype"}



{"int2oid","smallint"},
{"int2vectoroid","smallint[]"},
{"int4arrayoid","integer[]"},
{"textarrayoid","text[]"},
{"int8oid","bigint"},
{"float4oid","real"},
{"float8oid","double precision"},
{"int4oid","integer"},
{"regprocoid","regproc"},
{"textoid","text"},
{"oidoid","oid"},
{"tidoid","tid"},
{"xidoid","xid"},
{"cidoid","cid"},
{"oidvectoroid","oidvector"},
{"pg_typeoid","pg_type"},
{"pg_attributeoid","pg_attribute"},
{"pg_procoid","pg_proc"},
{"jsonboid","jsonb"},
{"xmloid","xml"},
{"float8oid","double precision"},
{"unknownoid","unknown"},
{"bpcharoid","character"},
{"varcharoid","character varying"},
{"dateoid","date"},
{"timeoid","time without time zone"},
{"timestampoid","timestamp without time zone"},
{"timestamptzoid","timestamp with time zone"},
{"intervaloid","interval"},
{"numericoid","numeric"},
{"voidoid","void"},
{"uuidoid","uuid"},
{"jsonoid","json"},
{"int4rangeoid","int4range"},
{"numrangeoid","numrange"},
{"tsrangeoid","tsrange"},
{"tstzrangeoid","tstzrange"},
{"daterangeoid","daterange"},
{"int8rangeoid","int8range"},
{"regprocedureoid","regprocedure"},
{"regoperoid","regoper"},
{"regoperatoroid","regoperator"},
{"regclassoid","regclass"},
{"regtypeoid","regtype"}
*/


/*
int DataTypeConversion::sociTypeToPgOid(int sociType){
    switch(sociType) {
        case soci::dt_string: {
            return VARCHAROID;
            break;
        }
        case soci::dt_date: {
            return DATEOID;
            break;
        }
        case soci::dt_double: {
            return FLOAT4OID;
            break;
        }
        case soci::dt_integer: {
            return INT4OID;
            break;
        }
        case soci::dt_long_long: {
            return OID
            break;
        }
        case soci::dt_unsigned_long_long: {
            break;
        }
        case soci::dt_blob: {
            break;
        }
        case soci::dt_xml: {
            break;
        }
    };
};
*/