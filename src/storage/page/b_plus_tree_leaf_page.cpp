//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  return array_[index].second;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastTo(LeafPage *node_right) {
  int len = GetSize();
  int len_right = node_right->GetSize();
  MappingType *add = node_right->GetArrayAdd();
  for (int i = len_right; i >= 1; i--) {
    add[i] = add[i - 1];
  }
  add[0] = array_[len - 1];
  node_right->IncreaseSize(1);
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstTo(LeafPage *node_left) {
  int len = GetSize();
  int len_left = node_left->GetSize();
  MappingType *add = node_left->GetArrayAdd();
  add[len_left] = array_[0];
  for (int i = 0; i < len - 1; i++) {
    array_[i] = array_[i + 1];
  }
  node_left->IncreaseSize(1);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArrayAdd() -> MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Copy(MappingType *array, int base, int len) {
  for (int i = 0; i < len; i++) {
    array_[i] = array[base + i];
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyAtIndex(KeyType key, KeyComparator &comparator_) -> int {
  auto target = std::lower_bound(array_, array_ + GetSize(), key,
                                 [&comparator_](const auto &pair, auto k) { return comparator_(pair.first, k) < 0; });
  return std::distance(array_, target);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator &comparator_, bool &IsSplit)
    -> bool {
  if (GetSize() == 0) {
    array_[0] = {key, value};
    IncreaseSize(1);
    IsSplit = false;
    return true;
  }
  int u = GetKeyAtIndex(key, comparator_);
  if (comparator_(key, array_[u].first) == 0) {
    IsSplit = false;
    return false;
  }
  for (int i = GetSize(); i > u; i--) {
    array_[i] = array_[i - 1];
  }
  array_[u] = {key, value};
  IncreaseSize(1);
  IsSplit = GetSize() >= GetMaxSize();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(KeyType key, KeyComparator &comparator_) -> bool {
  if (GetSize() == 0) {
    return false;
  }
  int u = GetKeyAtIndex(key, comparator_);
  if (u == GetSize()) {
    return false;
  }
  if (comparator_(key, array_[u].first) != 0) {
    return false;
  }
  for (int i = u; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndexByKey(KeyType key, KeyComparator &comparator_) -> int {
  int u = GetKeyAtIndex(key, comparator_);
  if (u == GetSize()) {
    return -1;
  }
  if (comparator_(key, array_[u].first) == 0) {
    return u;
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArrayByIndex(int index) -> MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllFrom(BPlusTreeLeafPage *node_right) {
  MappingType *array = node_right->GetArrayAdd();
  int len_right = node_right->GetSize();
  int len = GetSize();
  for (int i = len; i < len_right + len; i++) {
    array_[i] = array[i - len];
  }
  SetNextPageId(node_right->GetNextPageId());
  IncreaseSize(len_right);
  node_right->SetSize(0);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValueByKey(KeyType key, ValueType &value, KeyComparator &comparator_) -> bool {
  if (GetSize() == 0) {
    return false;
  }
  int u = GetKeyAtIndex(key, comparator_);
  if (u == GetSize()) {
    return false;
  }
  if (comparator_(key, array_[u].first) == 0) {
    value = array_[u].second;
    return true;
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & { return array_[index]; }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
