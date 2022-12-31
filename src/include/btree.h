#include <stdbool.h>
// Define the maximum number of keys that can be stored in a node
// 512 is the page size
#define MAX_ORDER (512/sizeof(int) * 100) // 12800

typedef struct Node {
  int num_keys;                          // The number of keys currently stored in the node
  bool is_leaf;
  int keys[MAX_ORDER];                   // The array of keys stored in the node
  size_t* values[MAX_ORDER];             // The array of keys stored in the node
  struct Node* children[MAX_ORDER + 1];    // The array of pointers to child nodes
} Node;

typedef struct BTree {
  Node* root;
} BTree;

BTree* btree_create();

void btree_insert(BTree* tree, int key, size_t value);

size_t* btree_range_query(BTree* t, int key1, int key2);

