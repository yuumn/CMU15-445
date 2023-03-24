#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"
#define SEARCH 1
#define INSERT 2
#define DELETE 3
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticPessimisticLock(const KeyType &key, int type, Transaction *transaction) -> Page * {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate root_page_id_ page");
  }
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (type == INSERT) {
    if (node->IsLeafPage()) {
      page->WLatch();
    } else {
      page->RLatch();
    }
    root_latch_.RUnlock();
  }
  if (type == DELETE) {
    if (node->IsLeafPage()) {
      page->WLatch();
    } else {
      page->RLatch();
    }
    root_latch_.RUnlock();
  }

  while (!node->IsLeafPage()) {
    auto node_internal = static_cast<InternalPage *>(node);
    size_t len = node_internal->GetSize();
    page_id_t value = node_internal->ValueAt(0);
    for (size_t i = 1; i < len; i++) {
      if (comparator_(key, node_internal->KeyAt(i)) >= 0) {
        value = node_internal->ValueAt(i);
      } else {
        break;
      }
    }
    assert(value > 0);
    auto new_page = buffer_pool_manager_->FetchPage(value);
    auto new_node = reinterpret_cast<BPlusTreePage *>(new_page->GetData());
    if (new_node->IsLeafPage()) {
      new_page->WLatch();
    } else {
      new_page->RLatch();
    }
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    node = new_node;
    page = new_page;
  }

  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  if (type == INSERT) {
    if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
      return page;
    }
  }
  if (type == DELETE) {
    if (leaf_page->GetSize() > leaf_page->GetMinSize()) {
      return page;
    }
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return nullptr;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageByKey(const KeyType &key, int type, Transaction *transaction) -> Page * {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate root_page_id_ page");
  }
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (type == SEARCH) {
    root_latch_.RUnlock();
    page->RLatch();
  } else if (type == INSERT) {
    page->WLatch();
    if ((node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1) ||
        (!node->IsLeafPage() && node->GetSize() < node->GetMaxSize())) {
      ReleaseLatch(transaction);
    }
  } else if (type == DELETE) {
    page->WLatch();
    if (node->GetSize() > 2) {  // root_min_size=2
      ReleaseLatch(transaction);
    }
  }
  while (!node->IsLeafPage()) {
    auto node_internal = static_cast<InternalPage *>(node);
    size_t len = node_internal->GetSize();
    page_id_t value = node_internal->ValueAt(0);
    for (size_t i = 1; i < len; i++) {
      if (comparator_(key, node_internal->KeyAt(i)) >= 0) {
        value = node_internal->ValueAt(i);
      } else {
        break;
      }
    }
    assert(value > 0);
    auto new_page = buffer_pool_manager_->FetchPage(value);
    auto new_node = reinterpret_cast<BPlusTreePage *>(new_page->GetData());
    if (new_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate next page");
    }
    if (type == SEARCH) {
      new_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(node_internal->GetPageId(), false);
    } else if (type == INSERT) {
      new_page->WLatch();
      transaction->AddIntoPageSet(page);
      if ((new_node->IsLeafPage() && new_node->GetSize() < new_node->GetMaxSize() - 1) ||
          (!new_node->IsLeafPage() && new_node->GetSize() < new_node->GetMaxSize())) {
        ReleaseLatch(transaction);
      }
    } else if (type == DELETE) {
      new_page->WLatch();
      transaction->AddIntoPageSet(page);
      if (new_node->GetSize() > new_node->GetMinSize()) {
        ReleaseLatch(transaction);
      }
    }
    node = new_node;
    page = new_page;
  }
  return page;
  // auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // node=static_cast<LeafPage*>(node);
  // return leaf_page;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return false;
  }
  Page *page = GetLeafPageByKey(key, SEARCH, transaction);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool is_find = node->GetValueByKey(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  page->RUnlatch();
  if (is_find) {
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetNewRootPage() -> InternalPage * {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto node = reinterpret_cast<InternalPage *>(page->GetData());
  node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetNewInternalPage(page_id_t parent_id) -> InternalPage * {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto node = reinterpret_cast<InternalPage *>(page->GetData());
  node->Init(page_id, parent_id, internal_max_size_);
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetNewLeafPage(page_id_t parent_id) -> LeafPage * {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  node->Init(page_id, parent_id, leaf_max_size_);
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetInternalPage(page_id_t page_id) -> Page * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate page_id page");
  }
  // auto node = reinterpret_cast<InternalPage *>(page->GetData());
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(page_id_t page_id) -> Page * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate page_id page");
  }
  // auto node = reinterpret_cast<LeafPage *>(page->GetData());
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBPlusTreePage(page_id_t page_id) -> BPlusTreePage * {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate page_id page");
  }
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInInternalParent(InternalPage *node, KeyType key, InternalPage *node_new,
                                            Transaction *transaction) {
  if (node->IsRootPage()) {
    auto node_root = GetNewRootPage();
    node_root->SetValueAt(0, node->GetPageId());
    node_root->SetKeyAt(1, key);
    node_root->SetValueAt(1, node_new->GetPageId());
    node_root->SetSize(2);
    node->SetParentPageId(node_root->GetPageId());
    node_new->SetParentPageId(node_root->GetPageId());
    buffer_pool_manager_->UnpinPage(node_root->GetPageId(), true);
    UpdateRootPageId(0);
    ReleaseLatch(transaction);
    return;
  }
  Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto node_parent = reinterpret_cast<InternalPage *>(page->GetData());
  if (node_parent->GetSize() < internal_max_size_) {
    node_parent->InsertNodeAfter(node->GetPageId(), key, node_new->GetPageId());
    ReleaseLatch(transaction);
    buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
    return;
  }
  auto *newchar = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (node_parent->GetSize() + 1)];
  auto new_internal_page = reinterpret_cast<InternalPage *>(newchar);
  std::memcpy(newchar, page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (node_parent->GetSize()));
  new_internal_page->InsertNodeAfter(node->GetPageId(), key, node_new->GetPageId());
  auto node_new_inter = SplitInternal(new_internal_page);
  KeyType key1 = node_new_inter->KeyAt(0);
  std::memcpy(page->GetData(), newchar, INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * node_parent->GetMinSize());
  InsertInInternalParent(node_parent, key1, node_new_inter, transaction);
  buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node_new_inter->GetPageId(), true);
  delete[] newchar;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeafParent(LeafPage *node, KeyType key, LeafPage *node_new, Transaction *transaction) {
  if (node->IsRootPage()) {
    auto node_root = GetNewRootPage();
    node_root->SetValueAt(0, node->GetPageId());
    node_root->SetKeyAt(1, key);
    node_root->SetValueAt(1, node_new->GetPageId());
    node_root->SetSize(2);
    node->SetParentPageId(node_root->GetPageId());
    node_new->SetParentPageId(node_root->GetPageId());
    buffer_pool_manager_->UnpinPage(node_root->GetPageId(), true);
    UpdateRootPageId(0);
    ReleaseLatch(transaction);
    return;
  }
  Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto node_parent = reinterpret_cast<InternalPage *>(page->GetData());
  if (node_parent->GetSize() < internal_max_size_) {
    node_parent->InsertNodeAfter(node->GetPageId(), key, node_new->GetPageId());
    ReleaseLatch(transaction);
    buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
    return;
  }
  auto *newchar = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (node_parent->GetSize() + 1)];
  auto new_internal_page = reinterpret_cast<InternalPage *>(newchar);
  std::memcpy(newchar, page->GetData(), INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (node_parent->GetSize()));
  new_internal_page->InsertNodeAfter(node->GetPageId(), key, node_new->GetPageId());
  auto node_new_inter = SplitInternal(new_internal_page);

  KeyType key1 = node_new_inter->KeyAt(0);
  std::memcpy(page->GetData(), newchar, INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * node_parent->GetMinSize());

  InsertInInternalParent(node_parent, key1, node_new_inter, transaction);
  buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node_new_inter->GetPageId(), true);
  delete[] newchar;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *node) -> LeafPage * {
  auto node_new = GetNewLeafPage(node->GetParentPageId());
  int n = node->GetSize();
  int leftsize = node->GetMinSize();
  int rightsize = n - leftsize;
  node_new->Copy(node->GetArrayAdd(), leftsize, rightsize);
  node->SetSize(leftsize);
  node_new->SetSize(rightsize);
  node_new->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(node_new->GetPageId());
  return node_new;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *node) -> InternalPage * {
  auto node_new = GetNewInternalPage(node->GetParentPageId());
  int n = node->GetSize();
  int leftsize = node->GetMinSize();
  int rightsize = n - leftsize;
  node_new->Copy(node->GetArrayAdd(), leftsize, rightsize);
  node->SetSize(leftsize);
  node_new->SetSize(rightsize);
  for (int i = 0; i < node_new->GetSize(); i++) {
    BPlusTreePage *node_2 = GetBPlusTreePage(node_new->ValueAt(i));
    node_2->SetParentPageId(node_new->GetPageId());
    buffer_pool_manager_->UnpinPage(node_2->GetPageId(), true);
  }
  return node_new;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_latch_.WLock();
  if (IsEmpty()) {
    BuildNewTree(key, value);
    root_latch_.WUnlock();
    return true;
  }
  root_latch_.WUnlock();
  root_latch_.RLock();
  Page *page1 = OptimisticPessimisticLock(key, INSERT, transaction);

  if (page1 != nullptr) {
    auto leafpage = reinterpret_cast<LeafPage *>(page1->GetData());
    bool is_split = false;
    leafpage->Insert(key, value, comparator_, is_split);
    page1->WUnlatch();
    buffer_pool_manager_->UnpinPage(page1->GetPageId(), true);
    return true;
  }
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  Page *page = GetLeafPageByKey(key, INSERT, transaction);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  bool is_split = false;
  if (node->Insert(key, value, comparator_, is_split)) {
    if (is_split) {
      auto node_new = SplitLeaf(node);
      auto key_min = node_new->KeyAt(0);
      InsertInLeafParent(node, key_min, node_new, transaction);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_new->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    } else {
      ReleaseLatch(transaction);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    }
    return true;
  }
  ReleaseLatch(transaction);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BuildNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(&root_page_id_);
  auto *leafnode = reinterpret_cast<LeafPage *>(page->GetData());
  leafnode->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  bool is_split = false;
  leafnode->Insert(key, value, comparator_, is_split);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  UpdateRootPageId(1);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) { RemoveNoMy(key, transaction); }
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.WLock();
  // transaction->AddIntoPageSet(nullptr);
  // root_latch_.RLock();
  if (IsEmpty()) {
    // ReleaseLatch(transaction);
    root_latch_.WUnlock();
    return;
  }
  root_latch_.WUnlock();
  // // ReleaseLatch(transaction);
  root_latch_.RLock();
  // // transaction->AddIntoPageSet(nullptr);

  Page *page1 = OptimisticPessimisticLock(key, DELETE, transaction);
  if (page1 != nullptr) {
    auto leafpage = reinterpret_cast<LeafPage *>(page1->GetData());
    leafpage->Remove(key, comparator_);
    page1->WUnlatch();
    buffer_pool_manager_->UnpinPage(page1->GetPageId(), true);
    return;
  }
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  Page *page = GetLeafPageByKey(key, DELETE, transaction);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  DeleteEntryLeaf(node, key, transaction);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  for (auto it : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(it);
  }
  // std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
  //               [&bpm = buffer_pool_manager_](const page_id_t page_id) { bpm->DeletePage(page_id); });
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntryLeaf(LeafPage *node, const KeyType &key, Transaction *transaction) {
  if (!node->Remove(key, comparator_)) {
    ReleaseLatch(transaction);
    return;
  }
  if (node->IsRootPage()) {
    if (node->GetSize() == 0) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
    }
    ReleaseLatch(transaction);
    return;
  }
  if (node->GetSize() >= node->GetMinSize()) {
    ReleaseLatch(transaction);
    return;
  }

  Page *page = GetInternalPage(node->GetParentPageId());
  auto node_parent = reinterpret_cast<InternalPage *>(page->GetData());
  auto idx = node_parent->ValueIndex(node->GetPageId());
  if (idx > 0) {
    Page *page_left = GetLeafPage(node_parent->ValueAt(idx - 1));
    page_left->WLatch();
    auto node_left = reinterpret_cast<LeafPage *>(page_left->GetData());
    if (node_left->GetSize() == node_left->GetMinSize()) {
      node_left->MoveAllFrom(node);
      DeleteEntryInternal(node_parent, idx, transaction);
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_left->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
    } else {
      node_left->MoveLastTo(node);
      node_parent->SetKeyAt(idx, node->KeyAt(0));
      ReleaseLatch(transaction);
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_left->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
    }
  } else if (idx != node_parent->GetSize() - 1) {
    Page *page_right = GetLeafPage(node_parent->ValueAt(idx + 1));
    page_right->WLatch();
    auto node_right = reinterpret_cast<LeafPage *>(page_right->GetData());
    if (node_right->GetSize() == node_right->GetMinSize()) {
      idx = node_parent->ValueIndex(node_right->GetPageId());
      node->MoveAllFrom(node_right);
      DeleteEntryInternal(node_parent, idx, transaction);
      transaction->AddIntoDeletedPageSet(node_right->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_right->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_right->GetPageId(), true);
    } else {
      node_right->MoveFirstTo(node);
      ReleaseLatch(transaction);
      node_parent->SetKeyAt(idx + 1, node_right->KeyAt(0));
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_right->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_right->GetPageId(), true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntryInternal(InternalPage *node, int index, Transaction *transaction) {
  node->Remove(index);
  if (node->IsRootPage()) {
    if (node->GetSize() == 1) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      page_id_t page_id = node->ValueAt(0);
      auto node_new_root = GetBPlusTreePage(page_id);
      root_page_id_ = node_new_root->GetPageId();
      node_new_root->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(node_new_root->GetPageId(), true);
    }
    ReleaseLatch(transaction);
    return;
  }
  if (node->GetSize() >= node->GetMinSize()) {
    ReleaseLatch(transaction);
    return;
  }
  Page *page = GetInternalPage(node->GetParentPageId());
  auto node_parent = reinterpret_cast<InternalPage *>(page->GetData());
  auto idx = node_parent->ValueIndex(node->GetPageId());
  if (idx > 0) {
    Page *page_left = GetInternalPage(node_parent->ValueAt(idx - 1));
    page_left->WLatch();
    auto node_left = reinterpret_cast<InternalPage *>(page_left->GetData());
    if (node_left->GetSize() == node_left->GetMinSize()) {
      node->MoveAllToLeft(node_left, node_parent->KeyAt(idx), buffer_pool_manager_);
      DeleteEntryInternal(node_parent, idx, transaction);
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_left->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
    } else {
      node_left->MoveLastTo(node, node_parent->KeyAt(idx), buffer_pool_manager_);
      node_parent->SetKeyAt(idx, node->KeyAt(0));
      ReleaseLatch(transaction);
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_left->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
    }
  } else if (idx != node_parent->GetSize() - 1) {
    Page *page_right = GetInternalPage(node_parent->ValueAt(idx + 1));
    page_right->WLatch();
    auto node_right = reinterpret_cast<InternalPage *>(page_right->GetData());
    if (node_right->GetSize() == node_right->GetMinSize()) {
      idx = node_parent->ValueIndex(node_right->GetPageId());
      node_right->MoveAllToLeft(node, node_parent->KeyAt(idx), buffer_pool_manager_);
      DeleteEntryInternal(node_parent, idx, transaction);
      transaction->AddIntoDeletedPageSet(node_right->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_right->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_right->GetPageId(), true);
    } else {
      node_right->MoveFirstTo(node, node_parent->KeyAt(idx + 1), buffer_pool_manager_);
      node_parent->SetKeyAt(idx + 1, node_right->KeyAt(0));
      ReleaseLatch(transaction);
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      page_right->WUnlatch();
      buffer_pool_manager_->UnpinPage(node_right->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, nullptr, nullptr, 0);
  }
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  root_latch_.RUnlock();
  page->RLatch();
  while (!node->IsLeafPage()) {
    auto node_internal = static_cast<InternalPage *>(node);
    page_id_t value = node_internal->ValueAt(0);
    auto new_page = buffer_pool_manager_->FetchPage(value);
    auto new_node = reinterpret_cast<BPlusTreePage *>(new_page->GetData());
    new_page->RLatch();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(node_internal->GetPageId(), false);
    node = new_node;
    page = new_page;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // node=static_cast<LeafPage*>(node);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, nullptr, nullptr, 0);
  }
  Page *page = GetLeafPageByKey(key, SEARCH, nullptr);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  int index = node->GetIndexByKey(key, comparator_);
  if (index == -1) {
    return INDEXITERATOR_TYPE(nullptr, nullptr, nullptr, 0);
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, node, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, nullptr, nullptr, 0);
  }
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  root_latch_.RUnlock();
  page->RLatch();
  while (!node->IsLeafPage()) {
    auto node_internal = static_cast<InternalPage *>(node);
    page_id_t value = node_internal->ValueAt(node_internal->GetSize() - 1);
    auto new_page = buffer_pool_manager_->FetchPage(value);
    auto new_node = reinterpret_cast<BPlusTreePage *>(new_page->GetData());
    new_page->RLatch();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(node_internal->GetPageId(), false);
    node = new_node;
    page = new_page;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // node=static_cast<LeafPage*>(node);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, leaf_page, leaf_page->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    Page *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      root_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
