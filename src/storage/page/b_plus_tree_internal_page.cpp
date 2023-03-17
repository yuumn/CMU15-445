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
  // std::cout << "InternalPage: page_id:" << page_id << '\n';
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index <= GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // assert(index<=GetSize());
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // assert(index<=GetSize());
  if (index >= GetSize()) {
    IncreaseSize(1);
  }
  array_[index].second = value;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> page_id_t {
  assert(index <= GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator &comparator_) {
  if (GetSize() == 0) {
    array_[0].first = key;
    array_[0].second = value;
    IncreaseSize(1);
    return;
  }
  int u = GetSize();
  for (int i = 1; i < GetSize(); i++) {
    int compare = comparator_(key, array_[i].first);
    if (compare == 1) {
      continue;
    }
    if (compare == -1) {
      u = i;
      break;
    }
  }
  for (int i = GetSize(); i > u; i--) {
    array_[i] = array_[i - 1];
  }
  array_[u].first = key;
  array_[u].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArrayAdd() -> MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Copy(MappingType *array, int base, int len) {
  for (int i = 0; i < len; i++) {
    array_[i] = array[base + i + 1];
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValueLeft(ValueType value_now, ValueType &value) -> bool {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value_now) {
      if (i == 0) {
        return false;
      }
      value = array_[i - 1].second;
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValueRight(ValueType value_now, ValueType &value) -> bool {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value_now) {
      if (i == GetSize() - 1) {
        return false;
      }
      value = array_[i + 1].second;
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyByValue(KeyType key, ValueType value) {
  for (int i = 1; i < GetSize(); i++) {
    if (array_[i].second == value) {
      array_[i].first = key;
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(ValueType &value) {
  int u = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (value == array_[i].second) {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Print() {
  std::cout << "PageId:" << GetPageId() << '\n';
  std::cout << "i:" << 0 << "       "
            << "  value:" << array_[0].second << '\n';
  for (int i = 1; i < GetSize(); i++) {
    std::cout << "i:" << i << "  key:" << array_[i].first << "  value:" << array_[i].second << '\n';
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
