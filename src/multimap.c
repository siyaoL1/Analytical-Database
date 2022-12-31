#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#include "multimap.h"
#include "cs165_api.h"

// return true if n is a prime number
bool prime(int n) {
    int i = 0;
    for (i = 2; i <= n / 2; i++) {
        if (n % i == 0) {
            break;
        }
    }
    if (i > n / 2) {
        return true;
    } else {
        return false;
    }
}

// The smallest prime after 1.3*tuple_num
int get_proper_size(int tuple_num) {
    int curr_size = 1.3 * tuple_num;

    while (!prime(curr_size)) {
        curr_size += 1;
    }

    return curr_size;
}

// Create a new multimap based on the given size of the keys
multimap* create_multimap(int tuple_num) {
    int size = get_proper_size(tuple_num);
    // int size = tuple_num;

    multimap* map = malloc(sizeof(multimap));
    map->keys = calloc(size, sizeof(int));
    map->values = calloc(size, sizeof(int*));
    map->values_size = calloc(size, sizeof(int));
    map->values_capacity = calloc(size, sizeof(int));
    map->size = size;

    // init the value lists and the capacity
    for (int i = 0; i < size; i++) {
        map->values[i] = malloc(sizeof(int));
        map->values_capacity[i] = 1;
    }
    return map;
}

int hash(int key, int size) {
    // We create size number of buckets
    return key % size;
}

int find_index(multimap* map, int index, int key) {
    // the index has been taken by other number
    while (map->values_size[index] != 0 && map->keys[index] != key) {
        index = (index + 1) % map->size;
    }
    return index;
}

// Insert a key value into the multimap
void insert_multimap(multimap* map, int key, int value) {
    int hash_value = hash(key, map->size);
    int index = find_index(map, hash_value, key);

    // insert key
    map->keys[index] = key;

    // The values list is full, need to realloc
    if (map->values_capacity[index] == map->values_size[index]) {
        map->values_capacity[index] *= 2;
        map->values[index] = realloc(map->values[index], map->values_capacity[index] * sizeof(int));
    }

    // insert value
    map->values[index][map->values_size[index]++] = value;
}

// Look up a key in the multimap and return the value list in the provided result.
void lookup_multimap(multimap* map, int key, Result* result) {
    int hash_value = hash(key, map->size);
    int index = find_index(map, hash_value, key);

    if (map->values_size[index] == 0) {
        result->num_tuples = 0;
    } else {
        result->num_tuples = map->values_size[index];
        result->payload = map->values[index];
    }
}

// Free the memory used by a multimap
void free_multimap(multimap* map) {
    for (int key = 0; key < map->size; key++) {
        free(map->values[key]);
    }
    free(map->keys);
    free(map->values);
    free(map->values_size);
    free(map->values_capacity);
    free(map);
}