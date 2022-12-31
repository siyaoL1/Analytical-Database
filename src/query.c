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
// #include <sys/sysinfo.h>
#include <pthread.h>

#include "utils.h"
#include "cs165_api.h"
#include "message.h"
#include "parse.h"
#include "query.h"
#include "hashset.h"
#include "multimap.h"

void log_result(Result* result) {
	(void)result;
	return;
	log_info("---- Result info ----\n datatype: %d, num_tuples: %d, payload: \n", result->data_type, result->num_tuples);
	size_t i = 0;
	int* payload = result->payload;
	for (i = 0; i < result->num_tuples; i++) {
		log_info("%d\n", payload[i]);
	}
	log_info("--- End of log ---\n");
}

Result* select_result(Result* column, Result* prev_position, int* low_pointer, int* high_pointer, Status* ret_status) {
	develop_log("Inside select_result\n");
	int* position = malloc(column->num_tuples * sizeof(int));
	int* data = column->payload;
	int* data_pos = prev_position->payload;
	int index = 0;

	if (low_pointer && high_pointer) {
		develop_log("low_pointer && high_pointer\n");
		int low = *low_pointer, high = *high_pointer;
		for (size_t i = 0; i < column->num_tuples; i++) {
			if (data[i] >= low && data[i] < high) {
				position[index++] = data_pos[i];
			}
		}
	} else if (low_pointer == NULL && high_pointer) {
		develop_log("low_pointer == NULL && high_pointer\n");
		int high = *high_pointer;
		for (size_t i = 0; i < column->num_tuples; i++) {
			if (data[i] < high) {
				position[index++] = data_pos[i];
			}
		}
	} else if (low_pointer && high_pointer == NULL) {
		develop_log("low_pointer && high_pointer == NULL\n");
		int low = *low_pointer;
		for (size_t i = 0; i < column->num_tuples; i++) {
			if (data[i] >= low) {
				position[index++] = data_pos[i];
			}
		}
	} else if (low_pointer == NULL && high_pointer == NULL) {
		develop_log("low_pointer == NULL && high_pointer == NULL\n");
		for (size_t i = 0; i < column->num_tuples; i++) {
			position[i] = data_pos[i];
		}
		index = column->num_tuples;
	}

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = index;
	result->payload = position;
	ret_status->code = OK;

	log_result(result);

	return result;
}


/**
 * Sequential Scan
 */
Result* select_column_scan(Column* column, int* low_pointer, int* high_pointer, Status* ret_status) {
	develop_log("Inside select_column_scan\n");
	int* position = malloc(column->row_count * sizeof(int));
	int index = 0;

	if (low_pointer && high_pointer) {
		develop_log("low_pointer && high_pointer\n");
		int low = *low_pointer, high = *high_pointer;
		for (size_t i = 0; i < column->row_count; i++) {
			if (column->data[i] >= low && column->data[i] < high) {
				position[index++] = i;
			}
		}
	} else if (low_pointer == NULL && high_pointer) {
		develop_log("low_pointer == NULL && high_pointer\n");
		int high = *high_pointer;
		for (size_t i = 0; i < column->row_count; i++) {
			if (column->data[i] < high) {
				position[index++] = i;
			}
		}
	} else if (low_pointer && high_pointer == NULL) {
		develop_log("low_pointer && high_pointer == NULL\n");
		int low = *low_pointer;
		for (size_t i = 0; i < column->row_count; i++) {
			if (column->data[i] >= low) {
				position[index++] = i;
			}
		}
	} else if (low_pointer == NULL && high_pointer == NULL) {
		develop_log("low_pointer == NULL && high_pointer == NULL\n");
		for (size_t i = 0; i < column->row_count; i++) {
			position[i] = i;
		}
		index = column->row_count;
	}

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = index;
	result->payload = position;
	ret_status->code = OK;

	log_result(result);
	return result;
}

/**
 * Binary serach the position of the target in array, if the target is not
 * found return the biggest number smaller or equal to the target.
 */
size_t binary_search(int array[], int size, int target) {
	size_t left = 0;
	size_t right = size - 1;

	while (left <= right) {
		size_t mid = (left + right) / 2;

		if (array[mid] == target) {
			return mid;
		} else if (target < array[mid]) {
			right = mid - 1;
		} else {
			left = mid + 1;
		}
	}

	return right;
}

/**
 * Select with sorted index
 */
Result* select_column_sorted_index(Column* column, int low, int high, Status* ret_status) {
	develop_log("Inside select_column_sorted_index\n");
	int* result_position = malloc(column->row_count * sizeof(int));
	int index = 0;

	int* data = column->index->values;
	size_t* position = column->index->positions;
	size_t left = binary_search(data, column->row_count, low);
	size_t right = binary_search(data, column->row_count, high);
	// To move the positinos to the left most of the duplicates.
	while (left > 0 && data[left] >= low) {
		left--;
	}
	if (data[left] != low) {
		left ++;
	}
	while (right > left && data[right] == high) {
		right--;
	}
	log_info("low:%d, high:%d\n", low, high);
	for (size_t row = left; row <= right; row++) {
		log_info("value is: %d\n", data[row]);
		result_position[index++] = position[row];
	}

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = index;
	result->payload = result_position;
	ret_status->code = OK;

	log_result(result);
	return result;
}

/**
 * Select on the column
 */
Result* select_column(Column* column, int* low_pointer, int* high_pointer, Status* ret_status) {
	// If a query is selecting on clustered column always use sorted index or btree index
	if (column->clustered) {
		// Doesn't have btree
		// if (column->sorted) {
			return select_column_sorted_index(column, *low_pointer, *high_pointer, ret_status);
		// }
	}
	// If the column has secondary index and the selectivity is low
	if (column->has_index && should_use_index(column, *low_pointer, *high_pointer)) {
		// Doesn't have btree
		// if (column->sorted) {
			return select_column_sorted_index(column, *low_pointer, *high_pointer, ret_status);
		// }
	}

	return select_column_scan(column, low_pointer, high_pointer, ret_status);
}


Result* fetch_column(Column* column, Result* position_result, Status* ret_status) {
	develop_log("Inside fetch_column\n");
	// develop_log("table->row_count: %d\n", table->row_count);
	int* values = malloc(position_result->num_tuples * sizeof(int));
	int* position = position_result->payload;

	for (size_t i = 0; i < position_result->num_tuples; i++) {
		values[i] = column->data[position[i]];
	}
	// develop_log("index: %d, position_result->num_tuples: %d\n", index, position_result->num_tuples);

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = position_result->num_tuples;
	result->payload = values;

	log_result(result);

	ret_status->code = OK;
	return result;
}

char* print(Result** results, int result_num, Status* ret_status) {
	int index = 0;
	int num_tuples = 0;

	for (int i = 0; i < result_num; i++) {
		num_tuples += results[i]->num_tuples;
	}

	char* result_string = malloc(num_tuples * (LONG_INT_LENGTH + 1));

	for (int i = 0; i < result_num; i++) {
		Result* result = results[i];

		if (i > 0) {
			index += sprintf(&result_string[index], "%s", ",");
		}

		if (result->data_type == INT) {
			int* payload = result->payload;
			log_info("payload: %d\n", *payload);
			for (size_t i = 0; i < result->num_tuples; i++) {
				index += sprintf(&result_string[index], "%d", payload[i]);
				if (i != result->num_tuples - 1) {
					index += sprintf(&result_string[index], "\n");
				}
			}
		} else if (result->data_type == FLOAT) {
			float* payload = result->payload;
			log_info("payload: %.2f\n", *payload);
			for (size_t i = 0; i < result->num_tuples; i++) {
				index += sprintf(&result_string[index], "%.2f", payload[i]);
				if (i != result->num_tuples - 1) {
					index += sprintf(&result_string[index], "\n");
				}
			}
		} else if (result->data_type == LONG) {
			long* payload = result->payload;
			log_info("payload: %ld\n", *payload);
			for (size_t i = 0; i < result->num_tuples; i++) {
				index += sprintf(&result_string[index], "%ld", payload[i]);
				if (i != result->num_tuples - 1) {
					index += sprintf(&result_string[index], "\n");
				}
			}
		} else if (result->data_type == DOUBLE) {
			double* payload = result->payload;
			log_info("payload: %.2f\n", *payload);
			for (size_t i = 0; i < result->num_tuples; i++) {
				index += sprintf(&result_string[index], "%.2f", payload[i]);
				if (i != result->num_tuples - 1) {
					index += sprintf(&result_string[index], "\n");
				}
			}
		}
		
	}

	ret_status->code = OK;
	return result_string;
}

Result* average(Result* column, Status* ret_status) {
	double* average_result = malloc(sizeof(double)); // Max length of a float number
	long sum = 0;
	int* payload = column->payload;

	for (size_t i = 0; i < column->num_tuples; i++) {
		sum += payload[i];
	}
	*average_result = (double)sum / (double)column->num_tuples;

	Result* result = malloc(sizeof(Result));
	result->data_type = DOUBLE;
	result->num_tuples = 1;
	result->payload = average_result;

	ret_status->code = OK;
	return result;
}

Result* sum(GeneralizedColumn* column, Status* ret_status) {
	long* sum_result = malloc(sizeof(long)); // Max length of a long int
	long temp_result = 0;

	if (column->column_type == RESULT) {
		//TODO: add support for different types
		int* payload = column->column_pointer.result->payload;
		size_t num_tuples = column->column_pointer.result->num_tuples;
		for (size_t i = 0; i < num_tuples; i++) {
			temp_result += payload[i];
		}
	} else {
		int* data = column->column_pointer.column->data;
		size_t row_count = column->column_pointer.column->row_count;
		for (size_t i = 0; i < row_count; i++) {
			temp_result += data[i];
		}
	}

	*sum_result = temp_result;
	log_info("sum_result is : %d", *sum_result);
	Result* result = malloc(sizeof(Result));
	result->data_type = LONG;
	result->num_tuples = 1;
	result->payload = sum_result;

	ret_status->code = OK;
	return result;

}

Result* add(Result* column_one, Result* column_two, Status* ret_status) {
	int* add_result = malloc(sizeof(int) * column_one->num_tuples);
	int* payload_one = column_one->payload;
	int* payload_two = column_two->payload;

	for (size_t i = 0; i < column_one->num_tuples; i++) {
		add_result[i] = payload_one[i] + payload_two[i];
	}

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = column_one->num_tuples;
	result->payload = add_result;

	ret_status->code = OK;
	return result;
}

Result* sub(Result* column_one, Result* column_two, Status* ret_status) {
	int* sub_result = malloc(sizeof(int) * column_one->num_tuples);
	int* payload_one = column_one->payload;
	int* payload_two = column_two->payload;

	for (size_t i = 0; i < column_one->num_tuples; i++) {
		sub_result[i] = payload_one[i] - payload_two[i];
	}

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = column_one->num_tuples;
	result->payload = sub_result;

	ret_status->code = OK;
	return result;
}

Result* min(Result* column, Status* ret_status) {
	int* min_result = malloc(sizeof(int));
	int* payload = column->payload;
	int min = payload[0];

	for (size_t i = 0; i < column->num_tuples; i++) {
		log_info("min: %d, payload[%d]: %d\n", min, i, payload[i]);
		if (min > payload[i]) {
			min = payload[i];
		}
	}
	*min_result = min;

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = 1;
	result->payload = min_result;

	log_result(result);
	log_info("min value is %d\n", *min_result);

	ret_status->code = OK;
	return result;
}

Result* max(Result* column, Status* ret_status) {
	int* max_result = malloc(sizeof(int));
	int* payload = column->payload;
	int max = payload[0];

	for (size_t i = 0; i < column->num_tuples; i++) {
		log_info("max: %d, payload[%d]: \n", max, i, payload[i]);
		if (max < payload[i]) {
			max = payload[i];
		}
	}
	*max_result = max;

	Result* result = malloc(sizeof(Result));
	result->data_type = INT;
	result->num_tuples = 1;
	result->payload = max_result;

	ret_status->code = OK;
	return result;
}

void free_thread_args(thread_argument* args) {
	Result** results = args->results;

	for (int query = 0; query < args->query_count; query++) {
		free(results[query]->payload);
		free(results[query]);
	}
	free(results);
	free(args);
}

void* select_task(void* arguments) {
	thread_argument* args = arguments;
	SelectOperator* operators = args->operators;
	int query_count = args->query_count;
	Column* column = args->column;
	size_t start_index = args->start_index;
	size_t end_index = args->end_index;
	size_t task_length = end_index - start_index;

	develop_log("Inside select_task\n");
	int* position[query_count];
	int index[query_count];
	memset(index, 0, query_count * sizeof(int));

	// Init postion lists.
	for (int i = 0; i < query_count; i++) {
		position[i] = malloc(task_length * sizeof(int));
		if (position[i] == NULL) {
			log_info("Memory allocation failed\n");
		}
	}

	for (size_t row = start_index; row < end_index; row++) {
		for (int query = 0; query < query_count; query++) {
			if (column->data[row] >= operators[query].low && column->data[row] < operators[query].high) {
				// log_info("data: %d, low: %d, high: %d\n", column->data[row], operators[query].low, operators[query].high);
				position[query][index[query]++] = row;
			}
		}
	}

	Result** results = malloc(sizeof(Result*) * query_count);
	// Init result list.
	for (int i = 0; i < query_count; i++) {
		results[i] = malloc(sizeof(Result));
		results[i]->data_type = INT;
		results[i]->num_tuples = index[i];
		results[i]->payload = position[i];
		// log_result(results[i]);
	}

	args->results = results;

	return NULL;
}

Result** shared_select(SelectOperator* operators, int query_count, Column* column, Status* ret_status) {
	develop_log("Inside shared_select\n");
	int rc;
	// Use all the cores avialable.
	// int number_of_cores = get_nprocs();
	int number_of_cores = 3;
	log_info("number of cores: %d\n", number_of_cores);
	pthread_t threads[number_of_cores];
	thread_argument* args_list[number_of_cores];

	int range = column->max - column->min;
	int task_range = range / number_of_cores;
	size_t curr_index = 0;

	// Init the threads.
	for (int i = 0; i < number_of_cores; i++) {
		thread_argument* args = malloc(sizeof(thread_argument));
		// Assign task arguments
		args->operators = operators;
		args->query_count = query_count;
		args->column = column;
		args->start_index = curr_index;
		curr_index += task_range;
		args->end_index = curr_index;
		if (i == number_of_cores - 1) {
			args->end_index = column->row_count;
		}

		// save the args for retrieving result.
		args_list[i] = args;

		// Create thread.
		rc = pthread_create(&threads[i], NULL, select_task, args);
		if (rc) {
			log_info("Unable to create thread, %d", rc);
			ret_status->code = ERROR;
			return NULL;
		}
	}

	// Wait for the threads.
	for (int i = 0; i < number_of_cores; i++) {
		rc = pthread_join(threads[i], NULL);
		if (rc) {
			log_info("Thread execution failed, %d", rc);
			ret_status->code = ERROR;
			return NULL;
		}
	}

	// Init results
	Result** results = malloc(sizeof(Result*) * query_count);
	for (int i = 0; i < query_count; i++) {
		results[i] = malloc(sizeof(Result));
		results[i]->data_type = INT;
	}

	// Init the position lists for each query
	int* positions[query_count];
	for (int query = 0; query < query_count; query++) {
		positions[query] = malloc(column->row_count * sizeof(int));
		if (positions[query] == NULL) {
			log_info("Memory allocation failed\n");
		}
	}

	// Move result from thread result to position lists
	for (int query = 0; query < query_count; query++) {
		curr_index = 0;
		for (int t_id = 0; t_id < number_of_cores; t_id++) {
			size_t num_tuples = args_list[t_id]->results[query]->num_tuples;
			int* position = args_list[t_id]->results[query]->payload;
			memcpy(positions[query] + curr_index, position, num_tuples * sizeof(int));
			curr_index += num_tuples;
		}
		results[query]->num_tuples = curr_index;
		results[query]->payload = positions[query];
		// log_result(results[query]);
	}

	// Free the thread results
	for (int t_id = 0; t_id < number_of_cores; t_id++) {
		free_thread_args(args_list[t_id]);
	}

	ret_status->code = OK;
	return results;
}

Result** nested_loop_join(Result* column_one, Result* position_one,
	Result* column_two, Result* position_two, Status* ret_status) {
	int size = PAGE_SIZE;
	int* valid_pos_one = malloc(size * sizeof(int));
	int* valid_pos_two = malloc(size * sizeof(int));
	int valid_pos_index = 0;
	log_result(column_one);
	log_result(position_one);
	log_result(column_two);
	log_result(position_two);


	for (size_t outer_val = 0; outer_val < column_one->num_tuples; outer_val++) {
		for (size_t inner_val = 0; inner_val < column_two->num_tuples; inner_val++) {
			if (((int*)(column_one->payload))[outer_val] == ((int*)(column_two->payload))[inner_val]) {
				// log_info("matched value: %d, index from column one: %d, index from column two: %d\n", ((int*)(column_one->payload))[outer_val], outer_val, inner_val);
				if (valid_pos_index == size) {
					size *= 2;
					valid_pos_one = realloc(valid_pos_one, size * sizeof(int));
					valid_pos_two = realloc(valid_pos_two, size * sizeof(int));
				}
				valid_pos_one[valid_pos_index] = ((int*)(position_one->payload))[outer_val];
				valid_pos_two[valid_pos_index] = ((int*)(position_two->payload))[inner_val];
				valid_pos_index++;
			}
		}
	}
	// Block nested loop join version
	// int int_in_page = PAGE_SIZE/sizeof(int);
	// int values[int_in_page];
	// int positions[int_in_page];

	// for (size_t outer_page = 0; outer_page < column_one->num_tuples; outer_page+=int_in_page) {
	// 	memcpy(values, ((int*)(column_one->payload)) + outer_page, int_in_page*sizeof(int));
	// 	memcpy(positions, ((int*)(position_one->payload)) + outer_page, int_in_page*sizeof(int));
	// 	for (size_t page_index = 0; page_index < int_in_page && outer_page + page_index < column_one->num_tuples; page_index++) {
	// 		for (size_t inner_val = 0; inner_val < column_two->num_tuples; inner_val++) {
	// 			if (values[page_index] == ((int*)(column_two->payload))[inner_val]) {
	// 				// log_info("matched value: %d, index from column one: %d, index from column two: %d\n", ((int*)(column_one->payload))[outer_val], outer_val, inner_val);
	// 				if (valid_pos_index == size) {
	// 					size *= 2;
	// 					valid_pos_one = realloc(valid_pos_one, size * sizeof(int));
	// 					valid_pos_two = realloc(valid_pos_two, size * sizeof(int));
	// 				}
	// 				valid_pos_one[valid_pos_index] = positions[page_index];
	// 				valid_pos_two[valid_pos_index] = ((int*)(position_two->payload))[inner_val];
	// 				valid_pos_index++;
	// 			}
	// 		}
	// 	}
	// }

	Result** results = malloc(2 * sizeof(Result*));
	results[0] = malloc(sizeof(Result));
	results[0]->data_type = INT;
	results[0]->num_tuples = valid_pos_index;
	results[0]->payload = valid_pos_one;

	results[1] = malloc(sizeof(Result));
	results[1]->data_type = INT;
	results[1]->num_tuples = valid_pos_index;
	results[1]->payload = valid_pos_two;

	ret_status->code = OK;
	return results;
}

Result** hash_join(Result* column_one, Result* position_one,
	Result* column_two, Result* position_two, Status* ret_status) {
	int size = PAGE_SIZE;
	int* valid_pos_one = malloc(size * sizeof(int));
	int* valid_pos_two = malloc(size * sizeof(int));
	int valid_pos_index = 0;
	log_result(column_one);
	log_result(position_one);
	log_result(column_two);
	log_result(position_two);

	multimap* map = create_multimap(column_one->num_tuples);
	for (size_t outer_val = 0; outer_val < column_one->num_tuples; outer_val++) {
		insert_multimap(map, ((int*)(column_one->payload))[outer_val], ((int*)(position_one->payload))[outer_val]);
	}

	Result* look_up_result = malloc(sizeof(Result));
	for (size_t inner_val = 0; inner_val < column_two->num_tuples; inner_val++) {
		lookup_multimap(map, ((int*)(column_two->payload))[inner_val], look_up_result);
		for (size_t result_index = 0; result_index < look_up_result->num_tuples; result_index++) {
			if (valid_pos_index == size) {
				size *= 2;
				valid_pos_one = realloc(valid_pos_one, size * sizeof(int));
				valid_pos_two = realloc(valid_pos_two, size * sizeof(int));
			}
			valid_pos_one[valid_pos_index] = ((int*)look_up_result->payload)[result_index];
			valid_pos_two[valid_pos_index] = ((int*)(position_two->payload))[inner_val];
			valid_pos_index++;
		}
	}

	Result** results = malloc(2 * sizeof(Result*));
	results[0] = malloc(sizeof(Result));
	results[0]->data_type = INT;
	results[0]->num_tuples = valid_pos_index;
	results[0]->payload = valid_pos_one;

	results[1] = malloc(sizeof(Result));
	results[1]->data_type = INT;
	results[1]->num_tuples = valid_pos_index;
	results[1]->payload = valid_pos_two;

	ret_status->code = OK;
	return results;
}





// 91
// 119
// 219
// 247
// 352
// 356
// 358
// 424
// 433
// 436
// 455
// 473
// 517
// 527
// 542
// 569
// 601
// 684
// 703
// 778
// 780
// 869
// 870
// 895
// 921
// 929
// 953
// 978

// 91
// 119
// 219
// 247
// 352
// 356
// 358
// 424
// 433
// 436
// 455
// 473
// 517
// 527
// 542
// 569
// 601
// 684
// 703
// 778
// 780
// 869
// 870
// 895
// 921
// 929
// 953
// 978