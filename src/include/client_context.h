#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"
#include "db_manager.h"

Table* lookup_table(char *name);

Result* lookup_result(ClientContext* context, char *name);

void update_variable(Result* prev_result, Result* new_result);

void free_client_context(ClientContext* context);

void add_result_to_context(ClientContext* context, char* name, Result* result, Status* ret_status);

void create_batch_operator(ClientContext* context);

void add_to_batch(ClientContext* context, DbOperator* select_operator, Status* ret_status);

DbOperator* get_batch_operator(ClientContext* context);

#endif
