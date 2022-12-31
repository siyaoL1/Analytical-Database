#ifndef HASHSET_H__
#define HASHSET_H__

#include "cs165_api.h"

// Define a structure to represent a hashset
typedef struct hashset {
  int *keys;
  int size;
} hashset;

int hash(int key, int size);

hashset *create_hashset(int size);

void free_hashset(hashset *set);

void insert_hashset(hashset *set, int key);

bool lookup_hashset(hashset *set, int key);

Result* get_hashset_elements(hashset* set);

#endif