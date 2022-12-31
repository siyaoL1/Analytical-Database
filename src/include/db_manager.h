
#ifndef DB_MANAGER_H
#define DB_MANAGER_H

#include "cs165_api.h"


/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    LOAD,
    SHUTDOWN,
    SELECT,
    FETCH,
    PRINT,
    AVG,
    RELATIONAL_INSERT,
    SUM,
    ADD,
    SUB,
    MIN, 
    MAX,
    BATCHED_SELECT,
    JOIN
} OperatorType;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
    _INDEX
} CreateType;

typedef enum SelectType {
    COLUMN_SELECT,
    RESULT_SELECT,
} SelectType;

typedef enum JoinType {
    NESTED_LOOP_JOIN,
    HASH_JOIN,
} JoinType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator {
    CreateType create_type; 
    char name[MAX_SIZE_NAME]; 
    Db* db;
    Table* table;
    Column* column;
    int col_count;
    bool clustered;
    bool sorted;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator {
    char file_name[MAX_SIZE_PATH];
} LoadOperator;
/*
 * necessary fields for Select
 */
typedef struct SelectOperator {
    SelectType select_type; 
    char handle[HANDLE_MAX_SIZE];
    int low;
    int high;
    int has_low;
    int has_high;
    Db* db;
    Table* table;
    Column* column;
    Result* col_result;
    Result* pos_result;
    Comparator* comparator;
} SelectOperator;
/*
 * necessary fields for Fetch
 */
typedef struct FetchOperator {
    char handle[HANDLE_MAX_SIZE];
    Db* db;
    Column* column;
    Result* position;
} FetchOperator;
/*
 * necessary fields for Print
 */
typedef struct PrintOperator {
    Result** results;
    size_t result_num;
} PrintOperator;
/*
 * necessary fields for Average
 */
typedef struct AverageOperator {
    char handle[HANDLE_MAX_SIZE];
    Result* column;
} AverageOperator;
/*
 * necessary fields for Sum
 */
typedef struct SumOperator {
    char handle[HANDLE_MAX_SIZE];
    GeneralizedColumn column;
} SumOperator;
/*
 * necessary fields for Add
 */
typedef struct AddOperator {
    char handle[HANDLE_MAX_SIZE];
    Result* column_one;
    Result* column_two;
} AddOperator;
/*
 * necessary fields for Sub
 */
typedef struct SubOperator {
    char handle[HANDLE_MAX_SIZE];
    Result* column_one;
    Result* column_two;
} SubOperator;
/*
 * necessary fields for Min
 */
typedef struct MinOperator {
    char handle[HANDLE_MAX_SIZE];
    Result* column;
} MinOperator;
/*
 * necessary fields for Max
 */
typedef struct MaxOperator {
    char handle[HANDLE_MAX_SIZE];
    Result* column;
} MaxOperator;
/*
 * necessary fields for Batched Select
 */
typedef struct BatchedSelectOperator {
    int query_count;
    int size;
    SelectOperator* operators;
} BatchedSelectOperator;
/*
 * necessary fields for Batched Select
 */
typedef struct JoinOperator {
    char handle_one[HANDLE_MAX_SIZE];
    char handle_two[HANDLE_MAX_SIZE];
    // Column one should be bigger than column two
    Result* column_one;
    Result* position_one;
    Result* column_two;
    Result* position_two;
    JoinType type;
} JoinOperator;
/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AverageOperator average_operator;
    SumOperator sum_operator;
    AddOperator add_operator;
    SubOperator sub_operator;
    MinOperator min_operator;
    MaxOperator max_operator;
    BatchedSelectOperator batched_select_operator;
    JoinOperator join_operator;
} OperatorFields;
/*
 * This shouholds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
    int batching_query;
    BatchedSelectOperator *batch_operator;
} ClientContext;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

extern Db *current_db;


/* 
 * Start Operations
 */

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

Status create_db(const char* db_name);

void create_table(Db* db, const char* name, size_t num_columns, Status *status);

void create_column(Table *table, char *name, bool sorted, Status *ret_status);

void create_index(Column *column, bool clustered, bool sorted, Status *ret_status);

void load_db(Db* db, const char* path, Status *status);

void start_data(Db* db, Table* table, Column* column, Status* ret_status);

void start_column(Db* db, Table* table, Status* ret_status);

void start_db(Status *status);

/* 
 * Index related functions
*/

void quicksort(int* values, size_t* positions, int low, int high);

int partition(int* values, size_t* positions, int low, int high);

void swap_value(int* a, int* b);

void swap_position(size_t* a, size_t* b);

void init_column_index(Column* column);

void reorder_column(Column* column, size_t* positions);

void build_clustered_index(Table* table, Column* column);

void build_unclustered_index(Column* column);

void build_index(Db* db);

/* 
 *  Shutdown Operation
 */

Status shutdown_server();

void save_data(Table* table, Column* column, Status* ret_status);

void save_column(Db* db, Table* table, Status* ret_status);

void save_db(Status *status);

void release_db(Status *status);



void insert_row(Table* table, int* row, Status* ret_status);

char** execute_db_operator(DbOperator* query);
void db_operator_free(DbOperator* query);

/* 
 * Helper functions
 */
void print_db();

void add_result_to_context(ClientContext* context, char* name, Result* result ,Status* ret_status);


/**
 * Look up functions
*/
Table* lookup_table(char *name);

Column* lookup_column(Table *table, char *name);

#endif /* DB_MANAGER_H */

