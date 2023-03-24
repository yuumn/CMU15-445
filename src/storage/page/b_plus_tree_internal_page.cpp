//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, it);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArrayAdd() -> MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastTo(InternalPage *node_right, KeyType key,
                                                BufferPoolManager *buffer_pool_manager) {
  int len = GetSize();
  int len_right = node_right->GetSize();
  node_right->SetKeyAt(0, key);
  MappingType *add = node_right->GetArrayAdd();
  for (int i = len_right; i >= 1; i--) {
    add[i] = add[i - 1];
  }
  add[0] = array_[len - 1];
  node_right->IncreaseSize(1);
  IncreaseSize(-1);
  auto page = buffer_pool_manager->FetchPage(array_[len - 1].second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(node_right->GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstTo(InternalPage *node_left, KeyType key,
                                                 BufferPoolManager *buffer_pool_manager) {
  int len = GetSize();
  int len_left = node_left->GetSize();
  MappingType *add = node_left->GetArrayAdd();
  array_[0].first = key;

  auto page = buffer_pool_manager->FetchPage(array_[0].second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(node_left->GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);

  add[len_left] = array_[0];
  node_left->IncreaseSize(1);
  IncreaseSize(-1);
  for (int i = 0; i < len - 1; i++) {
    array_[i] = array_[i + 1];
  }
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllToLeft(InternalPage *node_left, KeyType key,
                                                   BufferPoolManager *buffer_pool_manager) {
  array_[0].first = key;
  int len = GetSize();
  int len_left = node_left->GetSize();
  MappingType *add = node_left->GetArrayAdd();
  for (int i = len_left; i < len_left + len; i++) {
    add[i] = array_[i - len_left];
    auto page = buffer_pool_manager->FetchPage(array_[i - len_left].second);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(node_left->GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
  node_left->IncreaseSize(len);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(ValueType old_value, KeyType key, ValueType new_value) {
  int u = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == old_value) {
      u = i + 1;
      break;
    }
  }
  for (int i = GetSize(); i > u; i--) {
    array_[i] = array_[i - 1];
  }
  array_[u] = {key, new_value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Copy(MappingType *array, int base, int len) {
  for (int i = 0; i < len; i++) {
    array_[i] = array[base + i];
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
