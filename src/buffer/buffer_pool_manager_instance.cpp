//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
auto BufferPoolManagerInstance::GetFrameId(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.back();
    free_list_.pop_back();
    return true;
  }
  if (replacer_->Evict(frame_id)) {
    Page *page = pages_ + *frame_id;
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page_table_->Remove(page->page_id_);
    return true;
  }
  return false;
}
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;

  if (!GetFrameId(&frame_id)) {
    return nullptr;
  }
  Page *page = pages_ + frame_id;
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  replacer_->Remove(frame_id);
  page_table_->Remove(page->page_id_);
  page->ResetMemory();
  page->page_id_ = AllocatePage();
  page_table_->Insert(page->page_id_, frame_id);
  page->pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  *page_id = page->page_id_;
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = pages_ + frame_id;
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }
  if (!GetFrameId(&frame_id)) {
    return nullptr;
  }
  Page *page = pages_ + frame_id;
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  replacer_->Remove(frame_id);
  page_table_->Remove(page->page_id_);
  page->ResetMemory();
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page->page_id_, page->data_);
  page_table_->Insert(page->page_id_, frame_id);
  page->pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = pages_ + frame_id;
  if (page->pin_count_ == 0) {
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (!page->is_dirty_) {
    page->is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = pages_ + frame_id;
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  Page *page;
  for (auto node : replacer_->GetUnLruk()) {
    page = pages_ + node.first;
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  for (auto node : replacer_->GetUnCache()) {
    page = pages_ + node.first;
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  Page *page = pages_ + frame_id;
  if (page->pin_count_ > 0) {
    return false;
  }
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  free_list_.push_back(frame_id);
  page->is_dirty_ = false;
  page->page_id_ = -1;
  page->ResetMemory();
  page->pin_count_ = 0;
  replacer_->Remove(frame_id);
  page_table_->Remove(page->page_id_);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
