#ifndef MULTIMAP_H__
#define MULTIMAP_H__

#include "cs165_api.h"

// Define a structure to represent a multimap
typedef struct multimap {
  int *keys;
  int **values;
  int *values_size;
  int *values_capacity;
  int size;
} multimap;

multimap *create_multimap(int size);

void insert_multimap(multimap *map, int key, int value);

void lookup_multimap(multimap* map, int key, Result* result);

void free_multimap(multimap *map);

#endif