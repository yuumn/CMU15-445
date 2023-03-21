/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, LeafPage *leafnode, int index)
    : buffer_pool_manager_(bpm), leafnode_(leafnode), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leafnode_->GetNextPageId() == INVALID_PAGE_ID && index_ == leafnode_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leafnode_->GetArrayByIndex(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ < leafnode_->GetSize() - 1) {
    index_++;
  } else {
    if (leafnode_->GetNextPageId() != INVALID_PAGE_ID) {
      Page *page = buffer_pool_manager_->FetchPage(leafnode_->GetNextPageId());
      auto leaf_next_node = reinterpret_cast<LeafPage *>(page->GetData());
      buffer_pool_manager_->UnpinPage(leafnode_->GetPageId(), false);
      leafnode_ = leaf_next_node;
      index_ = 0;
    } else {
      index_++;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
