//
// Created by serdar on 6/8/23.
//

#ifndef RUNMAIN_PQ_H
#define RUNMAIN_PQ_H

#include "ccms.h"

#include <arrow/api.h>

class RunMain_pq {
public:
    static arrow::Status testPgAndArrow(std::string statement, StatementType statementType, bool colFirst);
    static arrow::Status RunMain();
};


#endif //RUNMAIN_PQ_H
