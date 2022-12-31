#include <stdio.h>
#include <stdlib.h>

#include "btree.h"


BTree* btree_create() {
    Node* node = malloc(sizeof(Node));
    node->num_keys = 0;
    node->is_leaf = true;
    for (int i = 0; i <= MAX_ORDER; i++) {
        node->children[i] = NULL;
    }

    BTree* btree = malloc(sizeof(BTree));
    btree->root = node;
    return btree;
}

void btree_free(Node* node) {
    if (node->is_leaf) {
        return;
    }
    for (int i = 0; i <= node->num_keys; i++) {
        btree_free(node->children[i]);
    }
    return;
}

// Insert a new key into the B+ tree
void btree_insert(BTree *btree, int key, size_t value) {
   
}
size_t* bptree_range_query(BTree *btree, int left, int right) {
    Node* node = btree->root;

    // traverse the tree to the leaf node containing the first key in the range
    while (!node->is_leaf) {
        int i = 0;
        while (i < node->num_keys && node->keys[i] < left) {
            i++;
        }
        node = node->children[i];
    }

    // search the leaf nodes for keys in the range
    while (node != NULL && node->keys[0] <= right) {
        // search this node for keys in the range
        for (int i = 0; i < node->num_keys; i++) {
            if (node->keys[i] >= left && node->keys[i] <= right) {
                //TODO: Add keys to the result
            }
        }

        // move to the next leaf node
        node = node->children[MAX_ORDER];
    }
}




