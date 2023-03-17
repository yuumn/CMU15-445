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
  // std::cout << "LeafPage: page_id:" << page_id << '\n';
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
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
  assert(index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValueByKey(KeyType key, ValueType &value, KeyComparator &comparator_) -> bool {
  int len = GetSize();
  if (len == 0) {
    return false;
  }
  for (int i = 0; i < len; i++) {
    int compare = comparator_(key, array_[i].first);
    if (compare == 1) {
      continue;
    }
    if (compare == -1) {
      return false;
    }
    if (compare == 0) {  // compare==0
      value = array_[i].second;
      return true;
    }
  }
  return false;
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator &comparator_, bool &IsSplit)
    -> bool {
  // true split sc
  // false no_split
  if (GetSize() == 0) {
    array_[0] = {key, value};
    IncreaseSize(1);
    IsSplit = false;
    return true;
  }
  int u = GetSize();
  for (int i = 0; i < GetSize(); i++) {
    int compare = comparator_(key, array_[i].first);
    if (compare == 1) {
      continue;
    }
    if (compare == -1) {
      u = i;
      break;
    }
    return false;
  }
  for (int i = GetSize(); i > u; i--) {
    array_[i] = array_[i - 1];
  }
  array_[u] = {key, value};
  IncreaseSize(1);
  IsSplit = GetSize() > GetMaxSize() - 1;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(KeyType key, KeyComparator &comparator_) {
  if (GetSize() == 0) {
    return;
  }
  int u = -1;
  for (int i = 0; i < GetSize(); i++) {
    if (comparator_(array_[i].first, key) == 0) {
      u = i;
      break;
    }
  }
  for (int i = u; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Print() {
  std::cout << "PageId:" << GetPageId() << '\n';
  for (int i = 0; i < GetSize(); i++) {
    std::cout << "i:" << i << "  key:" << array_[i].first << '\n';
  }
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
