//
// Created by serdar on 6/3/23.
//

#ifndef CCMS_H
#define CCMS_H

#include <map>
#include <string>
#include <vector>

enum StatementType {StandardQuery, SingleRow, CursorFetchAll, CursorFetchPartially, Insert, Update, Delete};
//enum FetchType {All, Partially};

enum DataSource {Postgresql, Arrow, Oracle, MSSQL, MySQL, MariaDB, Hadoop};

enum PGDataType {
  BOOLOID=16
};

extern  std::map<StatementType, std::string> StatementTypeMap;



/*
, BYTEAOID(17, byte[].class), CHAROID(18, Byte.class),
  NAMEOID(19, String.class), INT8OID(20, Long.class), INT2OID(21, Short.class), INT2VECTOROID(22, short[].class),
  INT4OID(23, Integer.class),
  REGPROCOID(24), TEXTOID(25, String.class), OIDOID(26, Integer.class), TIDOID(27), XIDOID(28), CIDOID(29),
  OIDVECTOROID(30),
  POINTOID(600), LSEGOID(601), PATHOID(602), BOXOID(603), POLYGONOID(604),
  LINEOID(628),
  FLOAT4OID(700, Float.class), FLOAT8OID(701, Double.class), 
  ABSTIMEOID(702), RELTIMEOID(703), TINTERVALOID(704), UNKNOWNOID(705),
  CIRCLEOID(718), CASHOID(790), CSTRINGOID(2275, String.class), 
  INETOID(869), CIDROID(650), BPCHAROID(1042, String.class), VARCHAROID(1043, String.class),
  DATEOID(1082, Date.class), TIMEOID(1083, Time.class), TIMESTAMPOID(1114, Timestamp.class),
  TIMESTAMPTZOID(1184, Timestamp.class), INTERVALOID(1186), TIMETZOID(1266, Time.class),
  ZPBITOID(1560), VARBITOID(1562), NUMERICOID(1700, BigDecimal.class), VOIDOID(2278, Void.class),
  BOOLARRAYOID(1000, boolean[].class ), ANYOID(2276, Object.class), ANYELEMENTOID(2283, Object.class),
  RECORDOID(2249, Object[].class), RECORDARRAYOID(2287, Object[][].class) ;
*/
#ifndef Elog
#define Elog(fmt, ...) elog(ERROR,(fmt),##__VA_ARGS__)
#endif	/* Elog() */

#endif //CCMS_H



