//
// Created by serdar on 6/2/23.
//


#include "include/main.h"
#include "include/schema_info.h"
#include "include/arrow_schema_info.h"
#include "include/arrow_backend.h"
#include "include/runmain_pq.h"
#include "include/generic_tools.h"

//#include <dbg.h>


#include <iostream>
#include <arrow/api.h>


#include "postgres.h"
#include <libpq-fe.h>
#if PG_VERSION_NUM < 110000
#include "catalog/pg_type.h"
#else
#include "catalog/pg_type_d.h"
#endif


int main() {
    // dbg(1, "Here we go...", true);
    GenericTools::PrintStats("Started");

    ccms_target::ArrowServer arrowServer;
    GenericTools::PrintStats("Arrow Server Defined");

    arrowServer.startArrowServer();
    GenericTools::PrintStats("Arrow Server Started");

    // std::cerr << "Libpq test" << std::endl << std::endl;
    // arrow::Status stpq = RunMain_pq::RunMain();
    // if (!stpq.ok()) {
    //     std::cerr << stpq << std::endl;
    //     return 1;
    // }
    // GenericTools::PrintStats("Run Main run");

    arrowServer.shutdownArrowServer();
    GenericTools::PrintStats("Arrow Server Shutdowned");

    return 0;
}