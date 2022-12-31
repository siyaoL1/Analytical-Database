/*
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "query.h"
#include "utils.h"
#include "client_context.h"

 /**
  * Takes a pointer to a string.
  * This method returns the original string truncated to where its first comma lies.
  * In addition, the original string now points to the first character after that comma.
  * This method destroys its input.
  **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status = INCORRECT_FORMAT;
    }
    return token;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

DbOperator* parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

DbOperator* parse_create_db(char* create_arguments) {
    char* token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}

/**
 * This method takes in a string representing the arguments to create a column in a database.
 * It parses those arguments, checks that they are valid, and creates a database in a database.
 **/

DbOperator* parse_create_col(char* create_arguments) {
    log_info("Inside parse_create_col.\n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status);
    char* db_table_name = next_token(create_arguments_index, &status);
    // char* sep = '.';

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    column_name = trim_quotes(column_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_table_name) - 1;
    if (db_table_name[last_char] != ')') {
        return NULL;
    }

    // replace the ')' with a null terminating character. 
    db_table_name[last_char] = '\0';
    char** db_table_name_index = &db_table_name;
    char* db_name = strsep(db_table_name_index, ".");
    char* table_name = strsep(db_table_name_index, ".");

    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }

    // Find the corresponding table 
    Table* current_table = current_db->tables;
    while (current_table && strcmp(current_table->name, table_name)) {
        current_table++;
    }

    if (!current_table) {
        cs165_log(stdout, "query unsupported. Bad table name");
        return NULL; //QUERY_UNSUPPORTED
    }
    log_info("table info { table name: %s, db capacity: %d, tables_size: %d\n", current_table->name, current_table->col_count, current_table->table_length);


    // make create dbo for column
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, column_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = current_table;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a index for a column in db.
 * It parses those arguments, checks that they are valid, and creates a index for a column.
*/
DbOperator* parse_create_index(char* create_arguments) {
    log_info("Inside parse_create_col.\n");
    message_status status = OK_DONE;
    create_arguments = trim_parenthesis(create_arguments);   
    char** create_arguments_index = &create_arguments;
    char* db_table_column_name = next_token(create_arguments_index, &status);
    char* sorted_or_btree = next_token(create_arguments_index, &status);
    char* clustered_or_unclustered = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }

    char ** db_table_column_name_index = &db_table_column_name;
    char* db_name = strsep(db_table_column_name_index, ".");
    char* table_name = strsep(db_table_column_name_index, ".");
    char* column_name = strsep(db_table_column_name_index, ".");

    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    
    // Find the corresponding table 
    Table* current_table = lookup_table(table_name);
    if (!current_table) {
        return NULL; //QUERY_UNSUPPORTED
    }
    log_table(current_table);

    // Find the corresponding column 
    Column* current_column = lookup_column(current_table, column_name);
    if (!current_table) {
        return NULL; //QUERY_UNSUPPORTED
    }
    log_column(current_column);

    bool sorted = false;
    // Parse clustered boolean
    if (strcmp(sorted_or_btree, "sorted") == 0) {
        sorted = true;
    }

    bool clustered = false;
    // Parse clustered boolean
    if (strcmp(clustered_or_unclustered, "clustered") == 0) {
        clustered = true;
    }
    
    // make create dbo for column
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _INDEX;
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = current_table;
    dbo->operator_fields.create_operator.column = current_column;
    dbo->operator_fields.create_operator.clustered = clustered;
    dbo->operator_fields.create_operator.sorted = sorted;
    return dbo;
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/

DbOperator* parse_create(char* create_arguments) {
    message_status mes_status = 0;
    DbOperator* dbo = NULL;
    char* tokenizer_copy, * to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments) + 1) * sizeof(char));
    char* token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy);
            } else if (strcmp(token, "idx") == 0) {
                dbo = parse_create_index(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

/**
 * This method takes in a string representing the arguments to load a database.
 * It parses those arguments, checks that they are valid, and load a database.
 **/

DbOperator* parse_load(char* load_arguments) {
    // load the database with the data at given filepath
    char* file_name = load_arguments;
    // trim quotes and check for finishing parenthesis.
    log_info("before define file_name\n");
    file_name = trim_quotes(trim_parenthesis(file_name));
    log_info("after define file_name, %s\n", file_name);


    // make create operator. 
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    log_info("before strcpy\n");
    strcpy(dbo->operator_fields.load_operator.file_name, file_name);
    // strcpy(dbo->operator_fields.create_operator.name, db_name);
    log_info("end of parse_load\n");
    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* db_table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        char** db_table_name_index = &db_table_name;
        char* db_name = strsep(db_table_name_index, ".");
        char* table_name = strsep(db_table_name_index, ".");
        if (!current_db || strcmp(current_db->name, db_name) != 0) {
            cs165_log(stdout, "query unsupported. Bad db name");
            return NULL; //QUERY_UNSUPPORTED
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free(dbo);
            return NULL;
        }
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_select reads in the arguments for a select statement and
 * then passes these arguments to a database function to select a column.
 **/

DbOperator* parse_select(char* select_arguments, char* handle, ClientContext* context) {
    // select column with given range
    log_info("Inside parse_select.\n");
    message_status status = OK_DONE;
    select_arguments = trim_parenthesis(select_arguments);
    char** select_arguments_index = &select_arguments;
    char* name = next_token(select_arguments_index, &status);

    // Make the select operator
    DbOperator* dbo = malloc(sizeof(DbOperator));
    SelectOperator* select_operator = &(dbo->operator_fields.select_operator);
    dbo->type = SELECT;

    if (strchr(name, '.')) {
        // column
        char** db_table_column_name_index = &name;
        char* db_name = strsep(db_table_column_name_index, ".");
        char* table_name = strsep(db_table_column_name_index, ".");
        char* column_name = strsep(db_table_column_name_index, ".");
        // check that the database argument is the current active database
        if (!current_db || strcmp(current_db->name, db_name) != 0) {
            log_info("current db name:%s, select db name: %s", current_db->name, db_name);
            cs165_log(stdout, "query unsupported. Bad db name");
            return NULL; //QUERY_UNSUPPORTED
        }
        // Find the corresponding table 
        log_info("table_name is: %s", table_name);
        Table* current_table = current_db->tables;
        while (current_table && strcmp(current_table->name, table_name)) {
            current_table++;
        }
        if (!current_table) {
            cs165_log(stdout, "query unsupported. Bad table name");
            return NULL; //QUERY_UNSUPPORTED
        }

        // Find the corresponding column
        Column* current_column = current_table->columns;
        while (current_column && strcmp(current_column->name, column_name)) {
            current_column++;
        }
        if (!current_column) {
            cs165_log(stdout, "query unsupported. Bad column name");
            return NULL; //QUERY_UNSUPPORTED
        }
        log_info("current_column->name: %s\n", current_column->name);

        log_info("select info => db name: %s, table name: %s, column name: %s\n", db_name, table_name, column_name);
        // Init operator_fields.
        select_operator->select_type = COLUMN_SELECT;
        select_operator->db = current_db;
        select_operator->table = current_table;
        select_operator->column = current_column;

    } else {
        // result
        char* col_name = next_token(select_arguments_index, &status);
        Result* pos_result = lookup_result(context, name);
        Result* col_result = lookup_result(context, col_name);
        select_operator->col_result = col_result;
        select_operator->pos_result = pos_result;
        select_operator->select_type = RESULT_SELECT;
    }

    char* low_string = next_token(select_arguments_index, &status);
    char* high_string = next_token(select_arguments_index, &status);
    log_info("low: %s, high: %s\n", low_string, high_string);

    if (strcmp(low_string, "null") == 0) {
        select_operator->low = 0;
        select_operator->has_low = 0;
    } else {
        select_operator->low = atoi(low_string);
        select_operator->has_low = 1;
    }

    if (strcmp(high_string, "null") == 0) {
        select_operator->high = 0;
        select_operator->has_high = 0;
    } else {
        select_operator->high = atoi(high_string);
        select_operator->has_high = 1;
    }

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    strcpy(select_operator->handle, handle);

    return dbo;
}

/**
 * parse_fetch reads in the arguments for a fetch statement and
 * then passes these arguments to a database function to fetch the data in a column.
 **/

DbOperator* parse_fetch(char* select_arguments, char* handle, ClientContext* context) {
    // fetch column with given position array
    log_info("Inside parse_fetch.\n");
    message_status status = OK_DONE;
    select_arguments = trim_parenthesis(select_arguments);
    char** select_arguments_index = &select_arguments;
    char* db_table_column_name = next_token(select_arguments_index, &status);
    char* name = next_token(select_arguments_index, &status);
    // char* sep = '.';

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }

    // Make the select operator
    DbOperator* dbo = malloc(sizeof(DbOperator));
    FetchOperator* fetch_operator = &(dbo->operator_fields.fetch_operator);
    dbo->type = FETCH;

    char** db_table_column_name_index = &db_table_column_name;
    char* db_name = strsep(db_table_column_name_index, ".");
    char* table_name = strsep(db_table_column_name_index, ".");
    char* column_name = strsep(db_table_column_name_index, ".");
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    // Find the corresponding table 
    Table* current_table = current_db->tables;
    while (current_table && strcmp(current_table->name, table_name)) {
        current_table++;
    }
    if (!current_table) {
        cs165_log(stdout, "query unsupported. Bad table name");
        return NULL; //QUERY_UNSUPPORTED
    }

    // Find the corresponding column
    Column* current_column = current_table->columns;
    while (current_column && strcmp(current_column->name, column_name)) {
        current_column++;
    }
    if (!current_column) {
        cs165_log(stdout, "query unsupported. Bad column name");
        return NULL; //QUERY_UNSUPPORTED
    }
    log_info("current_column->name: %s\n", current_column->name);

    // Find position from the client_context results
    Result* position;
    GeneralizedColumnHandle* handle_table;
    for (int i = 0; i < context->chandles_in_use; i++) {
        handle_table = context->chandle_table + i;
        // log_info("handle_table name: %s, position name: %s, strcpy(handle_table->name, name): %d\n", handle_table->name, name, strcpy(handle_table->name, name));
        if (strcmp(handle_table->name, name) == 0) {
            // log_info("Found the handle with the same name of: %s\n", handle_table->name);
            position = handle_table->generalized_column.column_pointer.result;
        }
    }

    log_info("fetch info => db name: %s, table name: %s, column name: %s, position name: %s\n", db_name, table_name, column_name, name);
    // Init operator_fields.
    fetch_operator->db = current_db;
    fetch_operator->column = current_column;
    fetch_operator->position = position;
    strcpy(fetch_operator->handle, handle);

    return dbo;
}

/**
 * parse_print reads in the arguments for a print statement and
 * then passes these arguments to a database function to print the data.
 **/

DbOperator* parse_print(char* print_arguments, ClientContext* context) {
    // print result with given name
    log_info("Inside parse_print.\n");
    print_arguments = trim_parenthesis(print_arguments);

    // not enough arguments
    if (strlen(print_arguments) == 0) {
        return NULL;
    }

    // Since strsep destroys input, we create a copy of the input. 
    char* print_arguments_copy, * to_free;
    print_arguments_copy = to_free = malloc((strlen(print_arguments) + 1) * sizeof(char));
    strcpy(print_arguments_copy, print_arguments);
    char** print_arguments_copy_index = &print_arguments_copy;

    // Find the number of result names in the query
    int count = 0;
    message_status status = OK_DONE;
    while (status != INCORRECT_FORMAT) {
        count++;
        next_token(print_arguments_copy_index, &status);
    }
    count--;
    log_info("count is:%d\n", count);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    PrintOperator* print_operator = &(dbo->operator_fields.print_operator);
    dbo->type = PRINT;
    print_operator->result_num = count;
    print_operator->results = malloc(count * sizeof(Result*));

    char** print_arguments_index = &print_arguments;

    for (int i = 0; i < count; i++) {
        char* name = next_token(print_arguments_index, &status);
        Result* result = lookup_result(context, name);
        if (result == NULL) {
            return NULL;
        }
        print_operator->results[i] = result;
    }

    // Logging result
    for (int i = 0; i < count; i++) {
        log_info("result%d\n", i);
        log_result(print_operator->results[i]);
    }

    free(to_free);

    return dbo;
}


/**
 * parse_average reads in the arguments for a average statement and
 * then passes these arguments to a database function to find the average of the data.
 **/

DbOperator* parse_average(char* average_arguments, char* handle, ClientContext* context) {
    // find the average of given variable.
    log_info("Inside parse_average.\n");
    char* name = trim_parenthesis(average_arguments);

    // not enough arguments
    if (strlen(average_arguments) == 0) {
        return NULL;
    }

    // Make the average operator
    DbOperator* dbo = malloc(sizeof(DbOperator));
    AverageOperator* average_operator = &(dbo->operator_fields.average_operator);
    dbo->type = AVG;

    // Find column from the client_context results
    Result* column = lookup_result(context, name);

    log_info("average info => result name: %s\n", name);
    // Init operator_fields.
    average_operator->column = column;
    strcpy(average_operator->handle, handle);

    return dbo;
}

/**
 * parse_average reads in the arguments for a average statement and
 * then passes these arguments to a database function to find the average of the data.
 **/

DbOperator* parse_sum(char* sum_arguments, char* handle, ClientContext* context) {
    // find the average of given variable.
    log_info("Inside parse_average.\n");
    char* name = trim_parenthesis(sum_arguments);

    // not enough arguments
    if (strlen(sum_arguments) == 0) {
        return NULL;
    }

    // Make the sum operator
    DbOperator* dbo = malloc(sizeof(DbOperator));
    SumOperator* sum_operator = &(dbo->operator_fields.sum_operator);
    strcpy(sum_operator->handle, handle);
    dbo->type = SUM;

    // Find column from the client_context results
    Result* variable = lookup_result(context, name);
    if (variable != NULL) {
        log_info("sum info => result name: %s\n", name);
        // Init operator_fields.
        sum_operator->column.column_pointer.result = variable;
        sum_operator->column.column_type = RESULT;

        // Find column to sum on
    } else {
        char** db_table_column_name_index = &name;
        char* db_name = strsep(db_table_column_name_index, ".");
        char* table_name = strsep(db_table_column_name_index, ".");
        char* column_name = strsep(db_table_column_name_index, ".");

        // check that the database argument is the current active database
        if (!current_db || strcmp(current_db->name, db_name) != 0) {
            cs165_log(stdout, "query unsupported. Bad db name");
            return NULL; //QUERY_UNSUPPORTED
        }
        // Find the corresponding table 
        Table* current_table = current_db->tables;
        while (current_table && strcmp(current_table->name, table_name)) {
            current_table++;
        }
        if (!current_table) {
            cs165_log(stdout, "query unsupported. Bad table name");
            return NULL; //QUERY_UNSUPPORTED
        }

        // Find the corresponding column
        Column* current_column = current_table->columns;
        while (current_column && strcmp(current_column->name, column_name)) {
            current_column++;
        }
        if (!current_column) {
            cs165_log(stdout, "query unsupported. Bad column name");
            return NULL; //QUERY_UNSUPPORTED
        }
        log_info("sum info => column name: %s\n", current_column->name);
        sum_operator->column.column_pointer.column = current_column;
        sum_operator->column.column_type = COLUMN;
    }


    return dbo;
}

DbOperator* parse_add(char* add_arguments, char* handle, ClientContext* context) {
    message_status status = 0;
    DbOperator* dbo = malloc(sizeof(DbOperator));

    add_arguments = trim_parenthesis(add_arguments);
    char** add_arguments_index = &add_arguments;
    char* col_one = next_token(add_arguments_index, &status);
    char* col_two = next_token(add_arguments_index, &status);

    dbo->type = ADD;
    dbo->operator_fields.add_operator.column_one = lookup_result(context, col_one);
    dbo->operator_fields.add_operator.column_two = lookup_result(context, col_two);
    strcpy(dbo->operator_fields.add_operator.handle, handle);

    return dbo;
}

DbOperator* parse_sub(char* sub_arguments, char* handle, ClientContext* context) {
    message_status status = 0;
    DbOperator* dbo = malloc(sizeof(DbOperator));

    sub_arguments = trim_parenthesis(sub_arguments);
    char** sub_arguments_index = &sub_arguments;
    char* col_one = next_token(sub_arguments_index, &status);
    char* col_two = next_token(sub_arguments_index, &status);

    dbo->type = SUB;
    dbo->operator_fields.sub_operator.column_one = lookup_result(context, col_one);
    dbo->operator_fields.sub_operator.column_two = lookup_result(context, col_two);
    strcpy(dbo->operator_fields.sub_operator.handle, handle);

    return dbo;
}

DbOperator* parse_min(char* min_arguments, char* handle, ClientContext* context) {
    log_info("Inside parse_min.\n");
    char* name = trim_parenthesis(min_arguments);

    // not enough arguments
    if (strlen(min_arguments) == 0) {
        return NULL;
    }

    // Make the average operator
    DbOperator* dbo = malloc(sizeof(DbOperator));
    MinOperator* min_operator = &(dbo->operator_fields.min_operator);
    dbo->type = MIN;

    // Find column from the client_context results
    Result* column = lookup_result(context, name);

    log_info("min info => result name: %s\n", name);
    // Init operator_fields.
    min_operator->column = column;
    strcpy(min_operator->handle, handle);

    return dbo;
}

DbOperator* parse_max(char* max_arguments, char* handle, ClientContext* context) {
    log_info("Inside parse_max.\n");
    char* name = trim_parenthesis(max_arguments);

    // not enough arguments
    if (strlen(max_arguments) == 0) {
        return NULL;
    }

    // Make the average operator
    DbOperator* dbo = malloc(sizeof(DbOperator));
    MaxOperator* max_operator = &(dbo->operator_fields.max_operator);
    dbo->type = MAX;

    // Find column from the client_context results
    Result* column = lookup_result(context, name);

    log_info("max info => result name: %s\n", name);
    // Init operator_fields.
    max_operator->column = column;
    strcpy(max_operator->handle, handle);

    return dbo;
}

DbOperator* parse_join(char* join_arguments, char* handle_one, char* handle_two, ClientContext* context) {
    message_status status = 0;
    DbOperator* dbo = malloc(sizeof(DbOperator));

    join_arguments = trim_parenthesis(join_arguments);
    char** join_arguments_index = &join_arguments;
    char* col_one = next_token(join_arguments_index, &status);
    char* pos_one = next_token(join_arguments_index, &status);
    char* col_two = next_token(join_arguments_index, &status);
    char* pos_two = next_token(join_arguments_index, &status);
    char* join_type = next_token(join_arguments_index, &status);

    Result* column_one = lookup_result(context, col_one);;
    Result* column_two = lookup_result(context, col_two);;
    Result* position_one = lookup_result(context, pos_one);;
    Result* position_two = lookup_result(context, pos_two);;

    // Put the larger column on one, smaller on two.
    if (column_one->num_tuples > column_two->num_tuples) {
        dbo->operator_fields.join_operator.column_one = column_one;
        dbo->operator_fields.join_operator.column_two = column_two;
        dbo->operator_fields.join_operator.position_one = position_one;
        dbo->operator_fields.join_operator.position_two = position_two;
        strcpy(dbo->operator_fields.join_operator.handle_one, handle_one);
        strcpy(dbo->operator_fields.join_operator.handle_two, handle_two);
    } else {
        dbo->operator_fields.join_operator.column_one = column_two;
        dbo->operator_fields.join_operator.column_two = column_one;
        dbo->operator_fields.join_operator.position_one = position_two;
        dbo->operator_fields.join_operator.position_two = position_one;
        strcpy(dbo->operator_fields.join_operator.handle_one, handle_two);
        strcpy(dbo->operator_fields.join_operator.handle_two, handle_one);
    }

    dbo->type = JOIN;
    dbo->operator_fields.join_operator.type = strcmp(join_type, "nested-loop") == 0 ? NESTED_LOOP_JOIN : HASH_JOIN;

    return dbo;
}



/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 *
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse?
 *      What if such command requires multiple arguments?
 **/

DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator* dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char* equals_pointer = strchr(query_command, '=');
    char* handle = query_command;
    char* handle_two = NULL;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }
    if (handle != NULL) {
        char* comma_pointer = strchr(handle, ',');
        if (comma_pointer != NULL) {
            // handle exists, store here. 
            *comma_pointer = '\0';
            handle_two = ++comma_pointer;
            cs165_log(stdout, "FILE HANDLE2: %s\n", handle_two);
        }
    }


    cs165_log(stdout, "\nQUERY: %s", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command);
        if (dbo == NULL) {
            send_message->status = INCORRECT_FORMAT;
        } else {
            send_message->status = OK_DONE;
        }
        log_info("-- parse_create\n");
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
        log_info("-- parse_insert\n");
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command);
        log_info("-- parse_load\n");
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        query_command += 4;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
        log_info("-- shutdown\n");
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        if (context->batching_query == 0) {
            dbo = parse_select(query_command, handle, context);
            log_info("-- select\n");
        } else {
            Status ret_status;
            // We are batching the select operator to execute later.
            add_to_batch(context, parse_select(query_command, handle, context), &ret_status);
            if (ret_status.code != OK) {
                log_info("Add select operator to batch failed.");
            }
            send_message->status = OK_DONE;
            log_info("-- batched select\n");
            return NULL;
        }
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, handle, context);
        log_info("-- fetch\n");
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, context);
        log_info("-- print\n");
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_average(query_command, handle, context);
        log_info("-- avg\n");
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_sum(query_command, handle, context);
        log_info("-- sum\n");
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_add(query_command, handle, context);
        log_info("-- add\n");
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_sub(query_command, handle, context);
        log_info("-- sub\n");
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_min(query_command, handle, context);
        log_info("-- min\n");
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_max(query_command, handle, context);
        log_info("-- max\n");
    } else if (strncmp(query_command, "batch_queries", 13) == 0) {
        context->batching_query = 1;
        create_batch_operator(context);
        log_info("-- batch_queries start\n");
        send_message->status = OK_DONE;
        return NULL;
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        context->batching_query = 0;
        dbo = get_batch_operator(context);
        log_info("-- batch_queries exeute\n");
    } else if (strncmp(query_command, "join", 4) == 0) {
        query_command += 4;
        dbo = parse_join(query_command, handle, handle_two, context);
        log_info("-- parse_join\n");
    }

    if (dbo == NULL) {
        return dbo;
    }

    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
