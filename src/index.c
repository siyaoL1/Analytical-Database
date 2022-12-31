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

#include "cs165_api.h"
#include "db_manager.h"
#include "utils.h"


/**
 * Quicksort: sort values in place and keep the positions updated.
*/
void quicksort(int* values, size_t* positions, int low, int high) {
	if (low < high) {
		int pivot = partition(values, positions, low, high);
		quicksort(values, positions, low, pivot - 1);
		quicksort(values, positions, pivot + 1, high);
	}
}

int partition(int* values, size_t* positions, int low, int high) {
	int pivot = values[high];
	int i = low - 1;
	for (int j = low; j < high; j++) {
		if (values[j] < pivot) {
			i++;
			swap_value(&values[i], &values[j]);
			swap_position(&positions[i], &positions[j]);
		}
	}
	swap_value(&values[i + 1], &values[high]);
	swap_position(&positions[i + 1], &positions[high]);
	return i + 1;
}

void swap_value(int* a, int* b) {
	int temp = *a;
	*a = *b;
	*b = temp;
}

void swap_position(size_t* a, size_t* b) {
	size_t temp = *a;
	*a = *b;
	*b = temp;
}

/**
 * Build a histogram for unclustered index or btree
*/
void build_histogram(Column *column) {
	Histogram * histogram = malloc(sizeof(Histogram));
	histogram->bin_size = (column->max - column->min) / (BIN_NUM - 1);
	memset(histogram->values, 0, sizeof(int) * BIN_NUM);
	memset(histogram->counts, 0, sizeof(size_t) * BIN_NUM);
	column->histogram = histogram;
	
	// Init bin
	size_t bin_start = 0;
	for (int bin = 0; bin < BIN_NUM; bin++) {
		histogram->values[bin] = bin_start;
		bin_start += histogram->bin_size;
	}
	// Updata counts for each bin
	for (size_t row = 0; row < column->row_count; row++) {
		int bin_number = (column->data[row] - column->min) / histogram->bin_size;
		if (bin_number >= 100) {
			log_info("bin number is : %d", bin_number);
		}
		histogram->counts[bin_number] ++;
	}
}

/**
 * Initialize the column index by create a sorted copy of the column data and the position list.
*/
void init_column_index(Column* column) {
	// Init column index 
	column->index = malloc(sizeof(ColumnIndex));
	column->index->values = malloc(column->row_count * sizeof(int));
	column->index->positions = malloc(column->row_count * sizeof(size_t));
	// Copy over the column data
	memcpy(column->index->values, column->data, column->row_count * sizeof(int));
	// Init original positions.
	for (size_t i = 0; i < column->row_count; i++) {
		column->index->positions[i] = i;
	}
}

/**
 * Reorder a column's data based on the position list.
*/
void reorder_column(Column* column, size_t* positions) {
	// Create a copy of the previous column, since we can't just replace the mmaped data array.
	int copy[column->row_count];
	memcpy(copy, column->data, column->row_count * sizeof(int));

	// Reorder the elements of the array
	for (size_t i = 0; i < column->row_count; i++) {
		 column->data[i] = copy[positions[i]];
	}
}

/**
 * Build a clustered sorted index 
*/
void build_clustered_index(Table* table, Column* column) {
	Column* curr_column;
	init_column_index(column);

	size_t* sorted_position = malloc(column->row_count * sizeof(size_t));
	memcpy(sorted_position, column->index->positions, column->row_count * sizeof(size_t));
	// Sort index values and keep the sorted positions
	quicksort(column->index->values, sorted_position, 0, column->row_count - 1);

	for (size_t j = 0; j < table->col_count; j++) {
		curr_column = &(table->columns[j]);
		if (strcmp(curr_column->name, column->name) != 0) {
			reorder_column(curr_column, sorted_position);
		}
	}
	free(sorted_position);
}

/**
 * Build a unclustered sorted index 
*/
void build_unclustered_index(Column* column) {
	init_column_index(column);
	quicksort(column->index->values, column->index->positions, 0, column->row_count - 1);
}

/**
 * Build a btree based on the sorted index.
*/
void build_btree() {
    
}

void build_index(Db* db) {
	Table* table;
	Column* column;

	for (size_t i = 0; i < db->tables_size; i++) {
		table = &(db->tables[i]);
		for (size_t j = 0; j < table->col_count; j++) {
			column = &(table->columns[j]);
			if (column->has_index) {
				if (column->clustered) {
					build_clustered_index(table, column);
					if (!column->sorted) {
						// Here clustered or unclusted are the same since the order are already propogated
						build_btree(column);
					}
				} else {
					build_unclustered_index(column);
					if (!column->sorted) {
						build_btree(column);
					}
					// Build histogram to decide whether to use unclustered index or not.
					build_histogram(column);
				}
			}
		}
	}
}

bool should_use_index(Column* column, int low, int high) {
	(void)column;
    (void)low;
    (void)high;
    return true;
}


