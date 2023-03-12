//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (Size() == 0) {
    return false;
  }
  for (auto it = history_.rbegin(); it != history_.rend(); it++) {
    if (it->flag_) {
      *frame_id = it->id_;
      un_history_.erase(it->id_);
      curr_size_--;
      auto frame = *it;
      history_.remove(frame);
      return true;
    }
  }
  auto it_min = cache_.begin();
  size_t fir = 0x3f3f3f3f;
  bool flag = false;
  for (auto it = cache_.begin(); it != cache_.end(); it++) {
    if (it->flag_) {
      if (it->q_record_.front() < fir) {
        fir = it->q_record_.front();
        it_min = it;
        flag = true;
      }
    }
  }
  if (!flag) {
    return false;
  }
  *frame_id = it_min->id_;
  un_cache_.erase(it_min->id_);
  auto frame = *it_min;
  cache_.remove(frame);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  current_timestamp_++;
  if (un_history_.find(frame_id) == un_history_.end() && un_cache_.find(frame_id) == un_cache_.end()) {
    if (Size() == replacer_size_) {
      if (!EvictNoLock()) {
        return;
      }
    }
    Frame frame(frame_id, 1, false, current_timestamp_);
    history_.push_front(frame);
    un_history_[frame_id] = history_.begin();
    return;
  }
  if (un_history_.find(frame_id) != un_history_.end()) {
    auto node = un_history_[frame_id];
    node->num_++;
    node->q_record_.push(current_timestamp_);
    if (node->num_ == k_) {
      Frame frame = *node;
      un_history_.erase(frame_id);
      history_.erase(node);
      cache_.push_front(frame);
      un_cache_[frame_id] = cache_.begin();
    }
  } else if (un_cache_.find(frame_id) != un_cache_.end()) {
    auto node = un_cache_[frame_id];
    node->num_++;
    node->q_record_.pop();
    // std::cout<<node->id_<<" "<<current_timestamp_<<" "<<node->q_record_.front()<<'\n';
    node->q_record_.push(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // std::cout<<"SetEvicatable:  frame_id:"<<frame_id<<" flag:"<<set_evictable<<'\n';
  if (un_history_.find(frame_id) != un_history_.end()) {
    bool flag = un_history_[frame_id]->flag_;
    un_history_[frame_id]->flag_ = set_evictable;
    if (flag && !set_evictable) {
      curr_size_--;
    } else if (!flag && set_evictable) {
      curr_size_++;
    }
  } else if (un_cache_.find(frame_id) != un_cache_.end()) {
    bool flag = un_cache_[frame_id]->flag_;
    un_cache_[frame_id]->flag_ = set_evictable;
    if (flag && !set_evictable) {
      curr_size_--;
    } else if (!flag && set_evictable) {
      curr_size_++;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // std::cout<<"Remove:  frame_id"<<frame_id<<'\n';
  if (un_history_.find(frame_id) != un_history_.end()) {
    if (un_history_[frame_id]->flag_) {
      auto node = un_history_[frame_id];
      history_.erase(node);
      curr_size_--;
      un_history_.erase(frame_id);
    }
  } else if (un_cache_.find(frame_id) != un_cache_.end()) {
    if (un_cache_[frame_id]->flag_) {
      auto node = un_cache_[frame_id];
      cache_.erase(node);
      curr_size_--;
      un_cache_.erase(frame_id);
    }
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::EvictNoLock() -> bool {
  // std::cout<<"Evict  ";
  if (Size() == 0) {
    // std::cout<<"error"<<'\n';
    return false;
  }
  for (auto it = history_.rbegin(); it != history_.rend(); it++) {
    // std::cout<<"id:"<<it->id_<<" flag:"<<it->flag_<<'\n';
    if (it->flag_) {
      // std::cout<<"1***"<<'\n';
      un_history_.erase(it->id_);
      // std::cout<<"3***"<<'\n';
      curr_size_--;
      auto frame = *it;
      history_.remove(frame);
      return true;
    }
  }
  auto it_min = cache_.begin();
  size_t fir = 0x3f3f3f3f;
  bool flag = false;
  for (auto it = cache_.begin(); it != cache_.end(); it++) {
    if (it->flag_) {
      if (it->first_record_ < fir) {
        fir = it->first_record_;
        it_min = it;
        flag = true;
      }
    }
  }
  if (!flag) {
    // std::cout<<"error"<<'\n';
    return false;
  }
  auto frame = *it_min;
  cache_.remove(frame);
  un_cache_.erase(it_min->id_);
  curr_size_--;
  return true;
}

}  // namespace bustub
