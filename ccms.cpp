#include "include/ccms.h"

std::map<StatementType, std::string> StatementTypeMap = {
    {StandardQuery, "Standard"},
    {SingleRow, "Single Row"},
    {CursorFetchAll, "Cursor Fetch All"},
    {CursorFetchPartially, "Cursor Fetch Partially"},
    {Insert, "Insert"},
    {Update, "Update"},
    {Delete, "Delete"}};


