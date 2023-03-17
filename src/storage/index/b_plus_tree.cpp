#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

#define printree                  \
  do {                            \
    Print1(buffer_pool_manager_); \
  } while (0)

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  std::cout << "leaf_max_size:" << leaf_max_size << '\n';
  std::cout << "internal_max_size:" << internal_max_size << '\n';
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageByKey(const KeyType &key) -> LeafPage * {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // buffer_pool_manager_->UnpinPage(root_page_id_, false);
  while (!node->IsLeafPage()) {
    auto node_internal = static_cast<InternalPage *>(node);
    size_t len = node_internal->GetSize();
    page_id_t value = node_internal->ValueAt(0);
    for (size_t i = 1; i < len; i++) {
      if (comparator_(key, node_internal->KeyAt(i)) >= 0) {
        value = node_internal->ValueAt(i);
      } else {
        // value=node_internal->ValueAt(i);
        break;
      }
    }
    buffer_pool_manager_->UnpinPage(node_internal->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(value);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // node=static_cast<LeafPage*>(node);
  return leaf_page;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // std::cout << "GetValue: key:" << key << '\n';
  if (IsEmpty()) {
    return false;
  }
  LeafPage *node = GetLeafPageByKey(key);
  ValueType value;
  bool is_find = node->GetValueByKey(key, value, comparator_);
  if (is_find) {
    result->push_back(value);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
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
  auto node = reinterpret_cast<InternalPage *>(page->GetData());
  node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetNewInternalPage(page_id_t parent_id) -> InternalPage * {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  auto node = reinterpret_cast<InternalPage *>(page->GetData());
  node->Init(page_id, parent_id, internal_max_size_);
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetNewLeafPage(page_id_t parent_id) -> LeafPage * {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  node->Init(page_id, parent_id, leaf_max_size_);
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetInternalPage(page_id_t page_id) -> InternalPage * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto node = reinterpret_cast<InternalPage *>(page->GetData());
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(page_id_t page_id) -> LeafPage * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBPlusTreePage(page_id_t page_id) -> BPlusTreePage * {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInInternalParent(InternalPage *node, KeyType key, InternalPage *node_new) {
  if (node->IsRootPage()) {
    auto node_root = GetNewRootPage();
    page_id_t page_id = node_root->GetPageId();
    node->SetParentPageId(page_id);
    node_new->SetParentPageId(page_id);
    node_root->SetValueAt(0, node->GetPageId());
    node_root->SetKeyAt(1, key);
    node_root->SetValueAt(1, node_new->GetPageId());
    root_page_id_ = node_root->GetPageId();
    buffer_pool_manager_->UnpinPage(node_root->GetPageId(), true);
    UpdateRootPageId(0);
    return;
  }
  InternalPage *node_1 = GetInternalPage(node->GetParentPageId());
  if (node_1->GetSize() < node->GetMaxSize()) {
    node_1->Insert(key, node_new->GetPageId(), comparator_);
    buffer_pool_manager_->UnpinPage(node_1->GetPageId(), true);
  } else {
    node_1->Insert(key, node_new->GetPageId(), comparator_);
    auto node_n = GetNewInternalPage(node->GetParentPageId());
    int n = node_1->GetMaxSize();
    int leftsize = n / 2 + 1;
    int rightsize = n - leftsize;
    node_n->Copy(node_1->GetArrayAdd(), leftsize, rightsize);
    node_1->IncreaseSize(-rightsize);
    node_n->SetSize(rightsize);
    for (int i = 0; i < node_n->GetSize(); i++) {
      InternalPage *node_2 = GetInternalPage(node_n->ValueAt(i));
      node_2->SetParentPageId(node_n->GetPageId());
      buffer_pool_manager_->UnpinPage(node_2->GetPageId(), true);
    }
    KeyType key_1 = node_n->KeyAt(0);
    InsertInInternalParent(node_1, key_1, node_n);
    buffer_pool_manager_->UnpinPage(node_1->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node_n->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeafParent(LeafPage *node, KeyType key, LeafPage *node_new) {
  if (node->IsRootPage()) {
    auto node_root = GetNewRootPage();
    page_id_t page_id = node_root->GetPageId();
    node->SetParentPageId(page_id);
    node_new->SetParentPageId(page_id);
    node_root->SetValueAt(0, node->GetPageId());
    node_root->SetKeyAt(1, key);
    node_root->SetValueAt(1, node_new->GetPageId());
    root_page_id_ = node_root->GetPageId();
    buffer_pool_manager_->UnpinPage(node_root->GetPageId(), true);
    UpdateRootPageId(0);
    return;
  }
  InternalPage *node_1 = GetInternalPage(node->GetParentPageId());
  if (node_1->GetSize() < node_1->GetMaxSize()) {
    node_1->Insert(key, node_new->GetPageId(), comparator_);
    buffer_pool_manager_->UnpinPage(node_1->GetPageId(), true);
  } else {
    node_1->Insert(key, node_new->GetPageId(), comparator_);
    auto node_n = GetNewInternalPage(node->GetParentPageId());
    int n = node_1->GetMaxSize();
    int leftsize = n / 2;
    int rightsize = n - leftsize;
    node_n->Copy(node_1->GetArrayAdd(), leftsize, rightsize);
    node_1->IncreaseSize(-rightsize);
    node_n->SetSize(rightsize);
    KeyType key = node_n->KeyAt(0);
    for (int i = 0; i < node_n->GetSize(); i++) {
      LeafPage *node_2 = GetLeafPage(node_n->ValueAt(i));
      node_2->SetParentPageId(node_n->GetPageId());
      buffer_pool_manager_->UnpinPage(node_2->GetPageId(), true);
    }
    InsertInInternalParent(node_1, key, node_n);
    buffer_pool_manager_->UnpinPage(node_1->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node_n->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // insert_num++;
  // std::cout << "*******************************Insert: key:" << key << " insert_num: " << insert_num << '\n';
  if (IsEmpty()) {
    BuildNewTree(key, value);
    //  printree;
    //  std::cout << " IsEmpty " << '\n';
    return true;
  }
  LeafPage *node = GetLeafPageByKey(key);
  bool is_split = false;
  if (node->Insert(key, value, comparator_, is_split)) {
    //  std::cout << "is_split: " << is_split << '\n';
    if (is_split) {
      page_id_t page_id;
      auto node_new = GetNewLeafPage(node->GetParentPageId());
      page_id = node_new->GetPageId();
      node_new->SetNextPageId(node->GetNextPageId());
      node->SetNextPageId(page_id);
      int n = leaf_max_size_;
      int leftsize = (n + 1) / 2;
      int rightsize = n - leftsize;
      node_new->Copy(node->GetArrayAdd(), leftsize, rightsize);
      node->IncreaseSize(-rightsize);
      node_new->SetSize(rightsize);
      KeyType key_min = node_new->KeyAt(0);
      InsertInLeafParent(node, key_min, node_new);
      buffer_pool_manager_->UnpinPage(node_new->GetPageId(), true);
    }
    // printree;
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return true;
  }
  //  std::cout << " Insert Error " << '\n';
  //  printree;
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
  UpdateRootPageId(0);
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::cout << "Remove: key:" << key << '\n';
  if (IsEmpty()) {
    return;
  }
  LeafPage *node = GetLeafPageByKey(key);
  DeleteEntryLeaf(node, key);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntryLeaf(LeafPage *node, const KeyType &key) {
  node->Remove(key, comparator_);
  if (node->IsRootPage()) {
    return;
  }
  if (node->GetSize() < node->GetMinSize()) {
    InternalPage *node_parent = GetInternalPage(node->GetParentPageId());
    page_id_t page_id;
    bool is_split = false;
    if (node_parent->GetValueLeft(node->GetPageId(), page_id)) {
      LeafPage *node_left = GetLeafPage(page_id);
      if (node_left->GetSize() == node->GetMinSize()) {
        for (int i = 0; i < node->GetSize(); i++) {
          node_left->Insert(node->KeyAt(i), node->ValueAt(i), comparator_, is_split);
        }
        node_left->SetNextPageId(node->GetNextPageId());
        DeleteEntryInternal(node_parent, node->GetPageId());
        buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
      } else {
        int len_left = node_left->GetSize();
        KeyType key = node_left->KeyAt(len_left - 1);
        node->Insert(key, node_left->ValueAt(len_left - 1), comparator_, is_split);
        node_left->Remove(key, comparator_);
        node_parent->SetKeyByValue(key, node->GetPageId());
        buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
      }
    } else if (node_parent->GetValueRight(node->GetPageId(), page_id)) {
      LeafPage *node_right = GetLeafPage(page_id);
      if (node->GetSize() + node_right->GetSize() <= node->GetMaxSize()) {
        for (int i = 0; i < node_right->GetSize(); i++) {
          node->Insert(node_right->KeyAt(i), node_right->ValueAt(i), comparator_, is_split);
        }
        node->SetNextPageId(node_right->GetNextPageId());
        DeleteEntryInternal(node_parent, node_right->GetPageId());
        buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(node_right->GetPageId(), true);
      } else {
        KeyType key = node_right->KeyAt(0);
        node->Insert(key, node_right->ValueAt(0), comparator_, is_split);
        node_right->Remove(key, comparator_);
        node_parent->SetKeyByValue(node_right->KeyAt(0), node_right->GetPageId());
        buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(node_right->GetPageId(), true);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntryInternal(InternalPage *node, page_id_t value) {
  node->Remove(value);
  if (node->IsRootPage()) {
    if (node->GetSize() == 1) {
      page_id_t page_id = node->ValueAt(0);
      auto node_new_root = GetBPlusTreePage(page_id);
      root_page_id_ = node_new_root->GetPageId();
      node_new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(node_new_root->GetPageId(), true);
      UpdateRootPageId(0);
    }
    return;
  }
  if (node->GetSize() >= node->GetMinSize()) {
    return;
  }
  InternalPage *node_parent = GetInternalPage(node->GetParentPageId());
  page_id_t page_id;
  if (node_parent->GetValueLeft(node->GetPageId(), page_id)) {
    InternalPage *node_left = GetInternalPage(page_id);
    if (node_left->GetSize() == node_left->GetMinSize()) {
      for (int i = 0; i < node->GetSize(); i++) {
        node_left->Insert(node->KeyAt(i), node->ValueAt(i), comparator_);
        auto node_in_left = GetBPlusTreePage(node->ValueAt(i));
        node_in_left->SetParentPageId(node_left->GetParentPageId());
      }
      DeleteEntryInternal(node_parent, node->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
    } else {
      int len_left = node_left->GetSize();
      page_id_t value = node_left->ValueAt(len_left - 1);
      KeyType key = node_left->KeyAt(len_left - 1);
      node->Insert(key, value, comparator_);
      node_left->Remove(value);
      node_parent->SetKeyByValue(key, node->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node_left->GetPageId(), true);
    }
  } else if (node_parent->GetValueRight(node->GetPageId(), page_id)) {
    InternalPage *node_right = GetInternalPage(page_id);
    if (node_right->GetSize() == node_right->GetMinSize()) {
      for (int i = 0; i < node_right->GetSize(); i++) {
        node->Insert(node_right->KeyAt(i), node_right->ValueAt(i), comparator_);
        auto node_in_left = GetBPlusTreePage(node_right->ValueAt(i));
        node_in_left->SetParentPageId(node->GetParentPageId());
      }
      DeleteEntryInternal(node_parent, node_right->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node_right->GetPageId(), true);
    } else {
      page_id_t value = node_right->ValueAt(0);
      KeyType key = node_right->KeyAt(0);
      node->Insert(key, value, comparator_);
      node_right->Remove(value);
      node_parent->SetKeyByValue(node_right->KeyAt(0), node_right->GetPageId());
      buffer_pool_manager_->UnpinPage(node_parent->GetPageId(), true);
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print1(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString1(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString1(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    std::cout << "IsLeafpage" << '\n';
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << " size: " << leaf->GetSize() << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << " size: " << internal->GetSize() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString1(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
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
