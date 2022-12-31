#define _DEFAULT_SOURCE
#include "client_context.h"
#include <string.h>
#include "cs165_api.h"
#include "utils.h"
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */

Result* lookup_result(ClientContext* context, char *name) {
	Result* result = NULL;
    GeneralizedColumnHandle* handle_table;
    for (int i = 0; i < context->chandles_in_use; i++) {
        handle_table = context->chandle_table + i;
        develop_log("current handle name: %s, expected name: %s, index: %d, strcmp(handle_table->name, name): %d\n", handle_table->name, name, context->chandles_in_use, strcmp(handle_table->name, name));
        if (strcmp(handle_table->name, name) == 0) {
            develop_log("Found the handle with the same name of: %s\n\n", handle_table->name);
            result = handle_table->generalized_column.column_pointer.result;
            return result;
        }
    }

	develop_log("Result with name %s is not found.\n\n", name);
	return NULL;
}

void update_result(Result* prev_result, Result* new_result) {
	if (prev_result != NULL) {
		// Free the previous payload
		if (prev_result->payload) {
			free(prev_result->payload);
		}
		prev_result->data_type = new_result->data_type;
		prev_result->num_tuples = new_result->num_tuples;
		prev_result->payload = new_result->payload;

		// Free the new result
		free(new_result);
		return;
	}
}

void add_result_to_context(ClientContext* context, char* name, Result* result, Status* ret_status) {
    develop_log("In add_result_to_context\n");
	Result* prev_result = lookup_result(context, name);
	if (prev_result != NULL) {
		update_result(prev_result, result);
		ret_status->code = OK;
		return;
	}

    if (context->chandles_in_use == context->chandle_slots) {
        context->chandle_slots *= 2;
        GeneralizedColumnHandle* chandle_table = realloc(context->chandle_table, context->chandle_slots * sizeof(GeneralizedColumnHandle));
        if (!chandle_table) {
            ret_status->code = ERROR;
            return;
        }
        context->chandle_table = chandle_table;
    }

    GeneralizedColumnHandle* handle = context->chandle_table + context->chandles_in_use;
    strcpy(handle->name, name);
    handle->generalized_column.column_type = RESULT;
    handle->generalized_column.column_pointer.result = result;
    develop_log("handle: %s, index: %d, column_type: %d, result: %d\n", handle->name, context->chandles_in_use, handle->generalized_column.column_type, handle->generalized_column.column_pointer.result);
    context->chandles_in_use ++;

    ret_status->code = OK;
}

void free_client_context(ClientContext* context) {
	int i = 0;

	for (i = 0; i < context->chandles_in_use; i++) {
		GeneralizedColumn column = (context->chandle_table + i)->generalized_column;
		if (column.column_type == RESULT) {
			free(column.column_pointer.result->payload);
			free(column.column_pointer.result);
		} else {
			free(column.column_pointer.column);
		}
	}
	free(context->chandle_table);
	free(context);
}

void create_batch_operator(ClientContext* context) {
	context->batch_operator = malloc(sizeof(BatchedSelectOperator));
	context->batch_operator->query_count = 0;
	context->batch_operator->size = 10;
	context->batch_operator->operators = malloc(sizeof(SelectOperator) * 10);
}

void add_to_batch(ClientContext* context, DbOperator* select_operator, Status* ret_status) {
	BatchedSelectOperator* batch_operator = context->batch_operator;

	if (batch_operator->query_count == batch_operator->size) {
        // Update the capacity and realloc space
        batch_operator->size *= 2;
        SelectOperator* operators = realloc(batch_operator->operators, batch_operator->size * sizeof(SelectOperator));
        if (!operators) {
            ret_status->code = ERROR;
        }
        batch_operator->operators = operators;
    }

	SelectOperator* new_operator = batch_operator->operators + batch_operator->query_count;
	memcpy(new_operator, &(select_operator->operator_fields.select_operator), sizeof(SelectOperator));
	batch_operator->query_count ++;


	ret_status->code=OK;
	log_info("Added select operator with name %s to batch\n", new_operator->handle);
}


DbOperator* get_batch_operator(ClientContext* context) {
    log_info("Inside parse_max.\n");
    // Make the average operator
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->operator_fields.batched_select_operator = *context->batch_operator;
    dbo->type = BATCHED_SELECT;

    return dbo;
}

void log_table(Table* table) {
    log_info("table info { name: %s, column count: %d, table length: %d\n", table->name, table->col_count, table->table_length);
}

void log_column(Column* column) {
    log_info("column info { name: %s, row count: %d clustered: %i, min: %d, max: %d\n", column->name, column->row_count, column->clustered, column->min, column->max);
}