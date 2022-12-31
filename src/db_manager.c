#define _DEFAULT_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#include "utils.h"
#include "cs165_api.h"
#include "message.h"
#include "parse.h"

#define DEFAULT_TABLE_CAPACITY 6
#define MAX_LINE_SIZE 1024

// In this class, there will always be only one active database at a time
Db* current_db;
// db_catalog contains the info of all the db, table_catalog contains info of the tables in a db, 
// and column catalog contains info of columns in a table
Catalog* db_catalog, * table_catalog, * column_catalog;

/*
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char* db_name) {
	develop_log("Inside create_db of %s\n", db_name);
	struct Status ret_status = { OK, NULL };
	Db* new_db = malloc(sizeof(Db));

	if (!new_db) {
		ret_status.code = ERROR;
		return ret_status;
	}

	// Initialize Db instance
	memset(new_db->name, 0, MAX_SIZE_NAME * sizeof(char));
	strcpy(new_db->name, db_name);
	new_db->tables_size = 0;
	new_db->tables_capacity = DEFAULT_TABLE_CAPACITY;	// -------------- What should be the capacity of the table be?
	new_db->tables = malloc(new_db->tables_capacity * sizeof(Table));
	for (size_t i = 0; i < new_db->tables_capacity; i++) {
		memset((new_db->tables + i)->name, 0, MAX_SIZE_NAME * sizeof(char));
	}
	if (new_db->tables == NULL) {
		// free new_db
		free(new_db);
		ret_status.code = ERROR;
		return ret_status;
	}

	// Assign the current_db to new_db
	current_db = new_db;

	ret_status.code = OK;
	return ret_status;
}

/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
void create_table(Db* db, const char* name, size_t num_columns, Status* ret_status) {
	// Check to see if the table_size exceeds the capacity
	develop_log("Inside create_table of %s\n", name);
	develop_log("Before: db info { db name: %s, db capacity: %d, tables_size: %d\n", db->name, db->tables_capacity, db->tables_size);
	if (db->tables_size == db->tables_capacity) {
		// Update the capacity and realloc space
		db->tables_capacity *= 2;
		Table* tables = realloc(db->tables, db->tables_capacity * sizeof(Table));
		if (!tables) {
			ret_status->code = ERROR;
			return;
		}
		db->tables = tables;
	}

	// Set new table as the end of the table array in the db
	Table* new_table = db->tables + db->tables_size;
	strcpy(new_table->name, name);
	new_table->col_count = num_columns;
	new_table->table_length = DEFAULT_COLUMN_LENGTH;//TODO resize
	new_table->row_count = 0;
	// Allocate space and set the memory to zero
	new_table->columns = calloc(new_table->col_count, sizeof(Column));
	if (!new_table->columns) {
		ret_status->code = ERROR;
		return;
	}
	db->tables_size += 1;

	ret_status->code = OK;
	develop_log("After: db info { db name: %s, db capacity: %d, tables_size: %d\n", db->name, db->tables_capacity, db->tables_size);
}

/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
void create_column(Table* table, char* name, bool sorted, Status* ret_status) {
	develop_log("Inside create_column of %s\n", name);
	// Find the location to insert the new column
	size_t col_num = 0;
	(void)sorted;

	while (col_num < table->col_count) {
		// If the current column is empty
		log_info("name of column:%s\n", table->columns[col_num].name);
		if (table->columns[col_num].name[0] == '\0') {
			break;
		}
		col_num++;
	}
	if (col_num >= table->col_count) {
		ret_status->code = ERROR;
		log_info("failed here.");
		return;
	}
	// Set new column to the first empty column in the table
	Column* new_column = table->columns + col_num;
	strcpy(new_column->name, name);
	new_column->row_count = 0;
	new_column->sorted = false;
	new_column->clustered = false;
	new_column->has_index = false;
	new_column->index = NULL;
	new_column->btree_node = NULL;
	new_column->histogram = NULL;
	new_column->max = INT_MIN;
	new_column->min = INT_MAX;
	start_data(current_db, table, new_column, ret_status);
	if (ret_status->code != OK) {
		ret_status->code = ERROR;
		log_info("failed here2.");
		return;
	}
	// Future: initialize index

	ret_status->code = OK;
}

/*
 * Here you will create a index object. The Status object can be used to return
 * to the caller that there was an error in index creation
 */
void create_index(Column* column, bool clustered, bool sorted, Status* ret_status) {
	develop_log("Inside create_index for column %s\n", column->name);

	// For now we don't do anything with index itself, we will build index after load.
	column->clustered = clustered;
	column->sorted = sorted;
	column->has_index = true;
	ret_status->code = OK;
}

void insert_row(Table* table, int* row, Status* ret_status) {
	// develop_log("table->row_count: %d, table->table_length: %d",table->row_count, table->table_length);
	// develop_log("table->row_count: %d",table->row_count);
	// develop_log("table->table_length: %d",table->table_length);
	if (table->row_count == table->table_length) {
		// Update the capacity and realloc space
		for (size_t i = 0; i < table->col_count; i++) {
			save_data(table, table->columns + i, ret_status);
			if (ret_status->code != OK) {
				develop_log("insert row into table: %s, row number: %d\n", table->name, table->row_count);
				develop_log("Failed insert row when expanding table_length for column: \n", i);
				return;
			}
		}
		table->table_length *= 2;
		for (size_t i = 0; i < table->col_count; i++) {
			start_data(current_db, table, table->columns + i, ret_status);
			if (ret_status->code != OK) {
				develop_log("insert row into table: %s, row number: %d\n", table->name, table->row_count);
				develop_log("Failed insert row when expanding table_length 2\n");
				return;
			}
		}
	}

	int new_row_idx = table->row_count++;
	for (size_t i = 0; i < table->col_count; i++) {
		table->columns[i].data[new_row_idx] = row[i];
		table->columns[i].row_count++;
		table->columns[i].max = table->columns[i].max > row[i] ? table->columns[i].max : row[i];
		table->columns[i].min = table->columns[i].min < row[i] ? table->columns[i].min : row[i];
		// log_info("max is: %d", table->columns[i].max);
	}

	ret_status->code = OK;
}

void print_db() {
	Db* db = current_db;
	Table* table;
	Column* column;
	char* info;

	if (!current_db) {
		develop_log("current_db is NULL\n");
		return;
	}

	info =
		"Current DB:\n"
		"	name: %s, tables_size: %d, tables_capacity: %d\n\n";
	develop_log(info, db->name, db->tables_size, db->tables_capacity);
	for (size_t i = 0; i < db->tables_size; i++) {
		table = &(db->tables[i]);
		info =
			"Table %d:\n"
			"	name: %s, col_count: %d, table_length: %d, row_count: %d\n\n";
		develop_log(info, i, table->name, table->col_count, table->table_length, table->row_count);

		for (size_t j = 0; j < table->col_count; j++) {
			column = &(table->columns[j]);
			info =
				"	Column %d:\n"
				"		name: %s, \n"
				"		max: %d, \n"
				"		min: %d, \n"
				"		data: (not printing)";
			develop_log(info, j, column->name, column->max, column->min);
			// for (size_t k = 0 ; k < table->row_count ; k++) {
			// 	develop_log("%d, ", column->data[k]);
			// }
			develop_log("\n");
		}
	}
}

void load_db(Db* db, const char* path, Status* ret_status) {
	develop_log("Inside load_db of %s1\n", path);
	// print_db();
	char db_name[MAX_SIZE_NAME];
	char table_name[MAX_SIZE_NAME];
	char line[MAX_LINE_SIZE];
	char* token, * temp;
	FILE* stream = fopen(path, "r");
	Table* table = NULL;
	ret_status->code = OK;

	develop_log("Inside load_db of %s2\n", path);

	if (!stream) {
		ret_status->code = ERROR;
		ret_status->error_message = "Failed to open file to load db";
		return;
	}

	develop_log("Inside load_db of %s3\n", path);

	// Parse the db name and table name. 
	// Note: Here I assumed that the order of column is consistent with existing table.
	fgets(line, MAX_LINE_SIZE, stream);
	temp = line;
	token = strsep(&temp, ".");
	strcpy(db_name, token);
	token = strsep(&temp, ".");
	strcpy(table_name, token);

	develop_log("Inside load_db of %s4, %s, %s, %s\n", path, db_name, table_name, db->name);

	// Check whether the db_name matches the db.
	if (!db || strcmp(db->name, db_name)) {
		develop_log("!strcmp(db->name, db_name): %d\n", strcmp(db->name, db_name));
		fclose(stream);
		ret_status->code = ERROR;
		return;
	}

	develop_log("Inside load_db of %s5\n", path);

	// Check if the table_name exist in the db. And get the corresponding table.
	for (size_t i = 0; i < db->tables_size; i++) {
		develop_log("table_name: %s, curr_table_name: %s\n", table_name, db->tables[i].name);
		if (!strcmp(db->tables[i].name, table_name)) {
			table = db->tables + i;
			break;
		}
	}

	if (!table) {
		fclose(stream);
		ret_status->code = ERROR;
		return;
	}

	develop_log("Inside load_db 6, table->col_count: %d\n", table->col_count);
	// Clear table
	// for (size_t i = 0; i < table->col_count; i++) {
	// 	memset(table->columns[i].data, 0, table->table_length * sizeof(int));
	// }

	// Parse row by row and insert into table.
	int row[table->col_count];

	while (fgets(line, MAX_LINE_SIZE, stream)) {
		temp = (char*)line;
		size_t index = 0;
		while ((token = strsep(&temp, ",")) != NULL && index < table->col_count) {
			row[index++] = atoi(token);
		}
		insert_row(table, row, ret_status);
		if (ret_status->code != OK) {
			fclose(stream);
			ret_status->code = ERROR;
			return;
		}
	}
	print_db();
	fclose(stream);
	ret_status->code = OK;
}

/*
 * Return the file path storing the db information of the system. => ./database/catalog.db
 */
char* get_db_file_path() {
	char* buffer = malloc(MAX_SIZE_PATH);
	sprintf(buffer, "./database/catalog.db");
	return buffer;
}

/*
 * Return the file path storing the table information of a db. e.g. ./database/db1.table
 */
char* get_table_file_path(char* db_name) {
	char* buffer = malloc(MAX_SIZE_PATH);
	sprintf(buffer, "./database/%s.table", db_name);
	return buffer;
}

/*
 * Return the file path storing the column information of a table. e.g. ./database/db1.tbl1.col
 */
char* get_column_file_path(char* db_name, char* table_name) {
	char* buffer = malloc(MAX_SIZE_PATH);
	sprintf(buffer, "./database/%s.%s.col", db_name, table_name);
	return buffer;
}

/*
 * Return the file path storing the data of a column. e.g. ./database/db1.tbl1.col1.data
 */
char* get_data_file_path(char* db_name, char* table_name, char* column_name) {
	char* buffer = malloc(MAX_SIZE_PATH);
	sprintf(buffer, "./database/%s.%s.%s.data", db_name, table_name, column_name);
	return buffer;
}

/*
 * Return the file path storing the sorted index of a column. e.g. ./database/db1.tbl1.col1.sorted_index
 */
char* get_sorted_index_file_path(char* db_name, char* table_name, char* column_name) {
	char* buffer = malloc(MAX_SIZE_PATH);
	sprintf(buffer, "./database/%s.%s.%s.sorted_index", db_name, table_name, column_name);
	return buffer;
}

/*
 * Return the file path storing the histogram of a column. e.g. ./database/db1.tbl1.col1.histogram
 */
char* get_histogram_file_path(char* db_name, char* table_name, char* column_name) {
	char* buffer = malloc(MAX_SIZE_PATH);
	sprintf(buffer, "./database/%s.%s.%s.histogram", db_name, table_name, column_name);
	return buffer;
}

/*
 * Save the sorted index data in .sorted_index file.
 */
void save_sorted_index(Db* db, Table* table, Column* column, Status* ret_status) {
	FILE* buffer;
	char* file_path;

	file_path = get_sorted_index_file_path(db->name, table->name, column->name);  // need to free file_path
	buffer = fopen(file_path, "w");
	free(file_path);
	if (!buffer) {
		develop_log("failed in save_sorted_index 1\n");
		develop_log("sorted index path is: %s\n", file_path);
		ret_status->code = ERROR;
		return;
	}

	// Write the sorted index
	fwrite(column->index->values, sizeof(int), column->row_count, buffer);
	fwrite(column->index->positions, sizeof(size_t), column->row_count, buffer);
	fclose(buffer);
	ret_status->code = OK;
}

/*
 * Save the histogram of the unclustered column index in .histogram file.
 */
void save_histogram(Db* db, Table* table, Column* column, Status* ret_status) {
	FILE* buffer;
	char* file_path;

	file_path = get_histogram_file_path(db->name, table->name, column->name);  // need to free file_path
	buffer = fopen(file_path, "w");
	free(file_path);
	if (!buffer) {
		develop_log("failed in save_sorted_index 1\n");
		develop_log("sorted index path is: %s\n", file_path);
		ret_status->code = ERROR;
		return;
	}

	// Write the sorted index
	fwrite(&(column->histogram->bin_size), sizeof(int), 1, buffer);
	fwrite(column->histogram->values, sizeof(int), BIN_NUM, buffer);
	fwrite(column->histogram->counts, sizeof(size_t), BIN_NUM, buffer);
	fclose(buffer);
	ret_status->code = OK;
}

/*
 * Save the column data in .data file.
 */
void save_data(Table* table, Column* column, Status* ret_status) {
	int rflag = -1;
	int capacity = table->table_length;

	if (column->data == NULL || column->fd == -1) {
		develop_log("failed in save_data 1\n");
		develop_log("column->data is: %d, column->fd is %d\n", column->data, column->fd);
		ret_status->code = ERROR;
		return;
	}

	/*Syncing the contents of the memory with file, flushing the pages to disk*/
	rflag = msync(column->data, capacity * sizeof(int), MS_SYNC);
	if (rflag == -1) {
		develop_log("Unable to msync. Errno: %s\n", strerror(errno));
		ret_status->code = ERROR;
		return;
	}


	// Write data
	// print_db();
	// for (size_t k = 0 ; k < table->row_count ; k++) {
	// 	develop_log("%d, ", column->data[k]);
	// }
	// develop_log("\n");
	ret_status->code = OK;
}

/*
 * Save the Column instances of a table in .col file.
 * Then call save_data to save the column data.
 */
void save_column(Db* db, Table* table, Status* ret_status) {
	FILE* buffer;
	char* file_path;
	Column* column;

	file_path = get_column_file_path(db->name, table->name);  // need to free file_path
	buffer = fopen(file_path, "w");
	free(file_path);
	if (!buffer) {
		develop_log("failed in save_column 1\n");
		develop_log("column path is: %s\n", file_path);
		ret_status->code = ERROR;
		return;
	}

	// Write the Columns
	for (size_t i = 0; i < table->col_count; i++) {
		column = table->columns + i;
		fwrite(column, sizeof(Column), 1, buffer);
		fwrite(column->name, sizeof(column->name), 1, buffer); // column_name
		// Save the data
		save_data(table, column, ret_status);
		if (column->has_index) {
			save_sorted_index(db, table, column, ret_status);
			// if (!column->sorted) {
			// 	save_btree_index();
			// }
			if (!column->clustered) {
				save_histogram(db, table, column, ret_status);
			}
		}
		if (ret_status->code != OK) {
			develop_log("failed in save_column 1\n");
			return;
		}
	}

	fclose(buffer);
	ret_status->code = OK;
}

/*
 * Save the Table instances of a db in .table file.
 * Then call save_column to save the Column instances.
 */
void save_table(Db* db, Status* ret_status) {
	FILE* buffer;
	char* file_path;
	Table* table;

	file_path = get_table_file_path(db->name);  // need to free file_path
	log_info("filepath is: %s\n", file_path);

	buffer = fopen(file_path, "w");
	free(file_path);
	if (!buffer) {
		develop_log("failed in save_table 1\n");
		develop_log("table path is: %s\n", file_path);
		ret_status->code = ERROR;
		return;
	}

	// Write the Tables
	for (size_t i = 0; i < db->tables_size; i++) {
		develop_log("i is :%d, db->tables_size is: %d\n", i, db->tables_size);
		table = db->tables + i;
		fwrite(table, sizeof(Table), 1, buffer); // table
		fwrite(table->name, sizeof(table->name), 1, buffer); // table_name
		// Save the column
		save_column(db, table, ret_status);
		if (ret_status->code != OK) {
			develop_log("failed in save_table 2\n");
			return;
		}
	}

	fclose(buffer);
	ret_status->code = OK;
}

/*
 * Save the db instances of the system to catalog.db file.
 * Then call save_table to save the Table instances.
 */
void save_db(Status* ret_status) {
	develop_log("Inside save_db 1\n");

	FILE* buffer;
	char* file_path;

	file_path = get_db_file_path();  // need to free file_path
	buffer = fopen(file_path, "w");
	develop_log("db path is: %s\n", file_path);
	free(file_path);
	if (!buffer) {
		develop_log("failed in save_db 1\n");
		// develop_log("db path is: %s\n", file_path);
		ret_status->code = ERROR;
		return;
	}

	// Write the db
	fwrite(current_db, sizeof(Db), 1, buffer);
	fwrite(current_db->name, sizeof(current_db->name), 1, buffer);
	// develop_log("Inside save_db %d\n", current_db->tables_size);

	// Save the table
	save_table(current_db, ret_status);
	if (ret_status->code != OK) {
		develop_log("failed in save_db 2\n");
		return;
	}

	fclose(buffer);
	ret_status->code = OK;
}

void release_index(ColumnIndex* index, Status* ret_status) {
	free(index->positions);
	free(index->values);
	ret_status->code = OK;
}

void release_btree(Node* btree_node, Status* ret_status) {
	// TODO: free btree.
	(void)btree_node;
	(void)ret_status;
}

void release_data(Table* table, Column* column, Status* ret_status) {
	int rflag = -1;
	int capacity = table->table_length;

	if (column->data != NULL && column->fd != -1) {
		/*Syncing the contents of the memory with file, flushing the pages to disk*/
		rflag = munmap(column->data, capacity * sizeof(int));
		if (rflag == -1) {
			develop_log("Unable to munmap.\n");
			ret_status->code = ERROR;
			ret_status->error_message = "Unable munmap";
			return;
		}
		close(column->fd);
	}
}

void release_column(Table* table, Status* ret_status) {
	for (size_t j = 0; j < table->col_count; j++) {
		Column* column = table->columns + j;
		release_data(table, column, ret_status);

		if (column->has_index) {
			release_index(column->index, ret_status);
			if (!column->sorted) {
				release_btree(column->btree_node, ret_status);
			}
			if (!column->clustered) {
				free(column->histogram);
			}
		}

		if (column->index) {
			free(column->index);
		}
		if (column->btree_node) {
			free(column->btree_node);
		}
		if (column->histogram) {
			free(column->histogram);
		}
	}
}

void release_table(Db* db, Status* ret_status) {
	for (size_t i = 0; i < db->tables_size; i++) {
		Table* table = db->tables + i;
		// Free Column
		release_column(table, ret_status);

		if (table->columns) {
			free(table->columns);
		}
	}
}

void release_db(Status* ret_status) {
	if (!current_db) {
		return;
	}

	// Save Db
	save_db(ret_status);
	if (ret_status->code != OK) {
		develop_log("failed in release_db, failed save db\n");
		return;
	}

	// Release Table
	release_table(current_db, ret_status);
	if (ret_status->code != OK) {
		develop_log("failed in release_db, failed release table 2\n");
		return;
	}
	if (current_db->tables != NULL) {
		free(current_db->tables);
	}

	free(current_db);
	current_db = NULL;
}

void start_index(Db* db, Table* table, Column* column, Status* ret_status) {
	develop_log("Inside start_index\n");
	FILE* buffer;
	char* file_path;

	file_path = get_sorted_index_file_path(db->name, table->name, column->name);  // need to free file_path
	buffer = fopen(file_path, "r");
	free(file_path);

	if (!buffer) {
		develop_log("failed in start_index 1\n");
		develop_log("sorted index path is: %s\n", file_path);
		ret_status->code = ERROR;
		return; // no column in db
	}

	// Init the index
	column->index = malloc(sizeof(ColumnIndex));
	column->index->values = malloc(column->row_count * sizeof(int));
	column->index->positions = malloc(column->row_count * sizeof(size_t));
	// Read the index
	fread(column->index->values, sizeof(int), column->row_count, buffer);
	fread(column->index->positions, sizeof(size_t), column->row_count, buffer);
	fclose(buffer);
	ret_status->code = OK;
}

void start_btree(Db* db, Table* table, Column* column, Status* ret_status) {
	(void)db;
	(void)table;
	(void)column;
	(void)ret_status;
}

void start_histogram(Db* db, Table* table, Column* column, Status* ret_status) {
	FILE* buffer;
	char* file_path;

	file_path = get_histogram_file_path(db->name, table->name, column->name);  // need to free file_path
	buffer = fopen(file_path, "w");
	free(file_path);
	if (!buffer) {
		develop_log("failed in start_histogram 1\n");
		develop_log("histogram is: %s\n", file_path);
		ret_status->code = ERROR;
		return;
	}

	// Init the histogram
	column->histogram = malloc(sizeof(Histogram));
	// Write the sorted index
	fread(&(column->histogram->bin_size), sizeof(int), 1, buffer);
	fread(column->histogram->values, sizeof(int), BIN_NUM, buffer);
	fread(column->histogram->counts, sizeof(size_t), BIN_NUM, buffer);
	fclose(buffer);
	ret_status->code = OK;
}

/*
 * Start the column data in .data file.
 * This could be called when a column is first created or it is loaded from the file.
 */
void start_data(Db* db, Table* table, Column* column, Status* ret_status) {
	develop_log("Inside start_data of db: %s, table: %s, column: %s\n", db->name, table->name, column->name);
	int fd;
	int rflag = -1;
	int capacity = table->table_length;
	int* data;
	char* file_path;

	// FILE* buffer;
	// Get the data file path
	file_path = get_data_file_path(db->name, table->name, column->name);  // need to free file_path
	// buffer = fopen(file_path, "rb");
	// free(file_path);
	// if (!buffer) {
	//     return; // no data in column
	// }
	// // Read data
	// fread(column->data, sizeof(int) * table->table_length, 1, buffer);
	// fclose(buffer);

	fd = open(file_path, O_RDWR | O_CREAT, (mode_t)0600);
	free(file_path);

	if (fd == -1) {
		develop_log("Opening the file failed.\n");
		return;
	}
	/* Moving the file pointer to the end of the file*/
	rflag = lseek(fd, (capacity * sizeof(int)) - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		develop_log("Lseek failed.\n");
		return;
	}
	/*Writing an empty string to the end of the file so that file is actually created and space is reserved on the disk*/
	rflag = write(fd, "", 1);
	if (rflag == -1) {
		close(fd);
		develop_log("Write failed.\n");
		return;
	}
	/*Mapping the contents of the file to memory and getting the address of the map */
	data = (int*)mmap(0, (capacity * sizeof(int)), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (data == MAP_FAILED) {
		close(fd);
		develop_log("Mmap failed.\n");
		return;
	}

	column->data = data;
	column->fd = fd;

	ret_status->code = OK;
}

/*
 * Start the Column instances of a table in .col file.
 * Then call start_data to start the column data.
 */
void start_column(Db* db, Table* table, Status* ret_status) {
	develop_log("Inside start_column\n");
	FILE* buffer;
	char* file_path;
	Column* column;

	file_path = get_column_file_path(db->name, table->name);  // need to free file_path
	buffer = fopen(file_path, "r");
	free(file_path);
	if (!buffer) {
		return; // no column in db
	}

	// Read the Columns
	for (size_t i = 0; i < table->col_count; i++) {
		column = table->columns + i;
		fread(column, sizeof(Column), 1, buffer);
		fread(column->name, sizeof(column->name), 1, buffer);
		// Start the data
		start_data(db, table, column, ret_status);
		if (ret_status->code != OK) {
			save_data(table, column, ret_status);
			return;
		}
		// Start index
		if (column->has_index) {
			start_index(db, table, column, ret_status);
			if (!column->sorted) {
				start_btree(db, table, column, ret_status);
			}
			if (!column->clustered) {
				start_histogram(db, table, column, ret_status);
			}
		}
	}

	fclose(buffer);
	ret_status->code = OK;
}

/*
 * Start the Table instances of a db in .table file.
 * Then call start_column to start the Column instances.
 */
void start_table(Db* db, Status* ret_status) {
	develop_log("Inside start_table\n");
	FILE* buffer;
	char* file_path;
	Table* table;

	file_path = get_table_file_path(db->name);  // need to free file_path
	develop_log("Inside start_table %s\n", file_path);
	buffer = fopen(file_path, "r");
	free(file_path);
	if (!buffer) {
		return; // no table in db
	}
	develop_log("Inside start_table1\n");
	// Read the Tables
	for (size_t i = 0; i < db->tables_size; i++) {
		table = db->tables + i;
		fread(table, sizeof(Table), 1, buffer);
		fread(table->name, sizeof(table->name), 1, buffer);
		// Init table's column array
		table->columns = calloc(table->col_count, sizeof(Column));
		if (table->columns == NULL) {
			ret_status->code = ERROR;
			return;
		}
		// Start the column
		start_column(db, table, ret_status);
		if (ret_status->code != OK) {
			free(table->columns);
			return;
		}
	}

	fclose(buffer);
	ret_status->code = OK;
}

void start_db(Status* ret_status) {
	develop_log("Inside start_db\n");

	if (current_db) {
		develop_log("db already started.\n");
		return;
	}

	Db* new_db = malloc(sizeof(Db));
	FILE* buffer;
	char* file_path;

	file_path = get_db_file_path(); // need to free file_path
	buffer = fopen(file_path, "r");
	free(file_path);
	develop_log("Inside start_db 2\n");
	if (!buffer) {
		return; // catalog file doesn't exist
	}
	develop_log("Inside start_db 3\n");

	// Read the db
	fread(new_db, sizeof(Db), 1, buffer);
	fread(new_db->name, sizeof(new_db->name), 1, buffer);
	fclose(buffer);
	develop_log("Inside start_db %d\n", new_db->tables_capacity);

	// Init the db tables array
	new_db->tables = malloc(new_db->tables_capacity * sizeof(Table));
	if (new_db->tables == NULL) {
		free(new_db);
		ret_status->code = ERROR;
		return;
	}

	// Start the table
	start_table(new_db, ret_status);
	if (ret_status->code != OK) {
		free(new_db);
		return;
	}

	// Set the db to newly started db.
	if (current_db) {
		release_db(ret_status);
		free(current_db);
	}
	if (ret_status->code != OK) {
		free(new_db);
		return;
	}
	current_db = new_db;
	ret_status->code = OK;
}


Table* lookup_table(char* name) {
	size_t idx = 0;
	Table* current_table;

	while (idx < current_db->tables_size) {
		current_table = current_db->tables + idx;
		if (strcmp(current_table->name, name) != 0) {
			develop_log("current table: %s, expected table: %s\n", current_table->name, name);
			idx++;
		} else {
			break;
		}
	}
	if (idx >= current_db->tables_size) {
		cs165_log(stdout, "query unsupported. Bad table name.");
		return NULL; //QUERY_UNSUPPORTED
	}

	return current_table;
}


Column* lookup_column(Table* table, char* name) {
	size_t idx = 0;
	Column* current_column;
	while (idx < table->col_count) {
		current_column = table->columns + idx;
		if (strcmp(current_column->name, name) != 0) {
			develop_log("current column: %s, expected column: %s\n", current_column->name, name);
			idx++;
		} else {
			break;
		}
	}
	if (idx >= table->col_count) {
		cs165_log(stdout, "query unsupported. Bad column name.");
		return NULL; //QUERY_UNSUPPORTED
	}

	return current_column;
}

// Status create_db_catalog() {
// 	develop_log("Inside create_db_catalog\n");
// 	struct Status ret_status = {OK, NULL};
// 	int fd;// file descritor of the db file

// 	// Catalog file path
// 	char file_path[]= "./database/catalog.data";

// 	// Creating the catalog file to disk
// 	fd = open(file_path, O_RDWR | O_CREAT, (mode_t)0600);
// 	if(fd == -1) {
// 		printf("Opening the catalog file failed.\n");
// 		ret_status.code = ERROR;
// 		return ret_status;
// 	}

// 	db_catalog->size = 0;
// 	db_catalog->capacity = DEFAULT_DB_CAPACITY;
// 	db_catalog->names = malloc(db_catalog->capacity * sizeof(*(db_catalog->names)));
// 	// Error check malloc
// 	db_catalog->file_path = malloc(db_catalog->capacity * sizeof(*(db_catalog->file_path)));
// 	// Error check malloc

// 	ret_status.code = OK;
// 	return ret_status;
// }

// Status load_db_catalog() {
// 	develop_log("Inside load_db_catalog\n");
// 	struct Status ret_status = {OK, NULL};
// 	int fd; // file descritor of the column file

// 	char file_path[]= "./database/catalog.data";
// 	size_t size_copied;

// 	// Check for existing catalog file
// 	if (access(file_path, F_OK) != 0) {
// 		return create_db_catalog();
// 	} 

// 	// Open the catalog file
// 	FILE *f = fopen(file_path, "r");
// 	fseek(f, 0, SEEK_END);
// 	long fsize = ftell(f);
// 	fseek(f, 0, SEEK_SET);

// 	// Read it into buffer
// 	char *buffer = malloc(fsize + 1);
// 	fread(buffer, fsize, 1, f);
// 	fclose(f);

// 	memcpy(&(db_catalog->size), buffer, sizeof(size_t));
// 	size_copied += sizeof(size_t);
// 	memcpy(&(db_catalog->capacity), buffer + size_copied, sizeof(size_t));
// 	size_copied += sizeof(size_t);
// 	memcpy(&(db_catalog->names), buffer, db_catalog->capacity * sizeof(*(db_catalog->names)));
// 	size_copied += db_catalog->capacity * sizeof(*(db_catalog->names));
// 	memcpy(&(db_catalog->size), buffer, db_catalog->capacity * sizeof(*(db_catalog->file_path)));

// 	free(buffer);
// }


// Todo: Add error check for the malloc.
