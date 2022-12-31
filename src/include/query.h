#ifndef QUERY_H
#define QUERY_H

#include "cs165_api.h"
#include "db_manager.h"

typedef struct thread_argument {
    SelectOperator* operators;
    int query_count;
    Column* column;
    size_t start_index;
    size_t end_index;
    Result** results;
} thread_argument;

/* 
 * Query Operation
 */

Result* select_result(Result *column, Result *position, int *low_pointer, int *high_pointer, Status *ret_status);

Result* select_column(Column *column, int *low, int *high, Status *ret_status);

Result* fetch_column(Column *column, Result* position_result, Status *ret_status);

char* print(Result** result, int result_num, Status *ret_status);

Result* average(Result* column, Status *ret_status);

Result* sum(GeneralizedColumn* column, Status *ret_status);

Result* add(Result* column_one, Result* column_two, Status *ret_status);

Result* sub(Result* column_one, Result* column_two, Status *ret_status);

Result* min(Result* column, Status *ret_status);

Result* max(Result* column, Status *ret_status);

Result** shared_select(SelectOperator* operators, int query_count, Column *column, Status *ret_status);

Result** nested_loop_join(Result* column_one, Result* position_one, Result* column_two, Result* position_two, Status *ret_status);

Result** hash_join(Result* column_one, Result* position_one, Result* column_two, Result* position_two, Status *ret_status);

/* 
 * Helper function
 */

void log_result(Result* result);


#endif