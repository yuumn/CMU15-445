//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

#include "common/rwlatch.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

enum class Operation { SEARCH, INSERT, DELETE };

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  void ReleaseLatch(Transaction *transaction);
  auto OptimisticPessimisticLock(const KeyType &key, int type, Transaction *transaction) -> Page *;
  auto GetLeafPageByKey(const KeyType &key, int type, Transaction *transaction) -> Page *;
  auto GetNewRootPage() -> InternalPage *;
  auto GetNewInternalPage(page_id_t parent_id) -> InternalPage *;
  auto GetNewLeafPage(page_id_t parent_id) -> LeafPage *;
  auto GetInternalPage(page_id_t page_id) -> Page *;
  auto GetLeafPage(page_id_t page_id) -> Page *;
  auto GetBPlusTreePage(page_id_t page_id) -> BPlusTreePage *;
  void InsertInInternalParent(InternalPage *node, KeyType key, InternalPage *node_new, Transaction *transaction);
  void InsertInLeafParent(LeafPage *node, KeyType key, LeafPage *node_new, Transaction *transaction);
  void BuildNewTree(const KeyType &key, const ValueType &value);
  // Insert a key-value pair into this B+ tree.
  // auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;
  auto SplitLeaf(LeafPage *node) -> LeafPage *;
  auto SplitInternal(InternalPage *node) -> InternalPage *;

  void DeleteEntryLeaf(LeafPage *node, const KeyType &key, Transaction *transaction);
  void DeleteEntryInternal(InternalPage *node, int index, Transaction *transaction);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  ReaderWriterLatch root_latch_;
};

}  // namespace bustub
