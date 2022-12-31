#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "hashset.h"
#include "cs165_api.h"
#include "multimap.h"
#include "utils.h"


// Create a new hashset
hashset* create_hashset(int size) {
    hashset* set = malloc(sizeof(hashset));
    set->keys = malloc(size * sizeof(int));
    set->size = size;
    return set;
}

// Free the memory used by a hashset
void free_hashset(hashset* set) {
    free(set->keys);
    free(set);
}

// Insert a key into the hashset
void insert_hashset(hashset* set, int key) {
    int index = hash(key, set->size);
    while (set->keys[index] != 0 && set->keys[index] != key) {
        index = (index + 1) % set->size;
    }
    set->keys[index] = key;
    log_info("current hashset size is: %d\n", set->size);
}

// Look up a key in the hashset and check if it exists in the set
bool lookup_hashset(hashset* set, int key) {
    int index = hash(key, set->size);
    while (set->keys[index] != 0 && set->keys[index] != key) {
        index = (index + 1) % set->size;
    }
    if (set->keys[index] == 0) {
        return false;
    } else {
        return true;
    }
}

// Get all of the existing elements in the hashset
Result* get_hashset_elements(hashset* set) {
    int* elements = malloc(sizeof(set->size) * sizeof(int));
    int count = 0;
    int i;
    for (i = 0; i < set->size; i++) {
        if (set->keys[i] != 0) {
            elements[count] = set->keys[i];
            count++;
        }
    }

    Result* result = malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = count;
    result->payload = elements;

    return result;
}