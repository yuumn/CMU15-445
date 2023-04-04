//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#define de(x) std::cout << (x) << "*******" << '\n'
namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    // 创建新的锁请求队列
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  // de(1);
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    // 该事务已有锁，进行锁升级
    if (lock_request->lock_mode_ == lock_mode) {
      lock_request_queue->latch_.unlock();
      return true;
    }
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      // 不允许多个事务在同一资源上同时尝试锁升级
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // 允许的锁升级
    // is -> [s, x, ix, six]
    // s -> [x, six]
    // ix -> [x, six]
    // SIX -> [X]
    if (lock_request->lock_mode_ == LockMode::INTENTION_SHARED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
          lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::SHARED) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    lock_request_queue->request_queue_.remove(lock_request);
    DeleteTableLockSet(txn, lock_request);

    // LockRequest *upgrade_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    auto lock_request_iter = lock_request_queue->request_queue_.begin();
    for (; lock_request_iter != lock_request_queue->request_queue_.end(); lock_request_iter++) {
      if (!(*lock_request_iter)->granted_) {
        break;
      }
    }

    lock_request_queue->request_queue_.insert(lock_request_iter, upgrade_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
    while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock);
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(upgrade_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    upgrade_lock_request->granted_ = true;
    InsertTableLockSet(txn, upgrade_lock_request);
    if (lock_mode != LockMode::EXCLUSIVE) {
      lock_request_queue->cv_.notify_all();
    }
    return true;
  }
  // de(4);
  // 添加锁到队列中
  // LockRequest *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  // de(5);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    // de(6);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // de(7);
  lock_request->granted_ = true;
  InsertTableLockSet(txn, lock_request);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (s_row_lock_set->find(oid) != s_row_lock_set->end()) {
    if (!s_row_lock_set->at(oid).empty()) {
      table_lock_map_latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                              AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  }
  if (x_row_lock_set->find(oid) != x_row_lock_set->end()) {
    if (!x_row_lock_set->at(oid).empty()) {
      table_lock_map_latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                              AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  }
  // de(1);
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      // de(3);
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
          lock_request->lock_mode_ == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      // de(6);
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          lock_request->lock_mode_ == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      // de(7);
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
          (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE) &&
          txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      DeleteTableLockSet(txn, lock_request);
      // de(5);
      return true;
    }
  }
  // de(2);
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }

  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if (lock_request->lock_mode_ == lock_mode) {
      lock_request_queue->latch_.unlock();
      return true;
    }
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // 允许的锁升级
    // is -> [s, x, ix, six]
    // s -> [x, six]
    // ix -> [x, six]
    // SIX -> [X]
    if (lock_request->lock_mode_ == LockMode::INTENTION_SHARED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
          lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::SHARED) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
    }
    if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    lock_request_queue->request_queue_.remove(lock_request);
    DeleteRowLockSet(txn, lock_request);

    // LockRequest *upgrade_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    auto lock_request_iter = lock_request_queue->request_queue_.begin();
    for (; lock_request_iter != lock_request_queue->request_queue_.end(); lock_request_iter++) {
      if (!(*lock_request_iter)->granted_) {
        break;
      }
    }

    lock_request_queue->request_queue_.insert(lock_request_iter, upgrade_lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
    while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
      lock_request_queue->cv_.wait(lock);
      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(upgrade_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    upgrade_lock_request->granted_ = true;
    InsertRowLockSet(txn, upgrade_lock_request);
    if (lock_mode != LockMode::EXCLUSIVE) {
      lock_request_queue->cv_.notify_all();
    }
    return true;
  }

  // 添加锁到队列中
  // LockRequest *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  // de(5);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    // de(6);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // de(7);
  lock_request->granted_ = true;
  InsertRowLockSet(txn, lock_request);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
          lock_request->lock_mode_ == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          lock_request->lock_mode_ == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
          (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE) &&
          txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      DeleteRowLockSet(txn, lock_request);
      return true;
    }
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = waits_for_[t1].begin();
  for (; iter != waits_for_[t1].end(); iter++) {
    if (*iter == t2) {
      break;
    }
  }
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (const auto &it_txn_id : txn_set_) {
    if (Dfs(it_txn_id)) {
      *txn_id = *active_set_.begin();
      for (const auto &active_set_id : active_set_) {
        *txn_id = std::max(*txn_id, active_set_id);
      }
      active_set_.clear();
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &it : waits_for_) {
    for (const auto &se : it.second) {
      edges.emplace_back(it.first, se);
    }
  }
  return edges;
}

void LockManager::DeleteTxn(txn_id_t txn_id) {
  waits_for_.erase(txn_id);
  for (const auto &other_id : txn_set_) {
    if (other_id == txn_id) {
      continue;
    }
    RemoveEdge(other_id, txn_id);
  }
}

auto LockManager::Dfs(txn_id_t txn_id) -> bool {
  if (safe_set_.find(txn_id) != safe_set_.end()) {
    return false;
  }
  active_set_.insert(txn_id);
  std::sort(waits_for_[txn_id].begin(), waits_for_[txn_id].end());
  for (const auto &next_id : waits_for_[txn_id]) {
    if (active_set_.find(next_id) != active_set_.end()) {
      return true;
    }
    if (Dfs(next_id)) {
      return true;
    }
  }
  active_set_.erase(txn_id);
  safe_set_.insert(txn_id);
  return false;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();

      for (const auto &it : table_lock_map_) {
        it.second->latch_.lock();
        std::unordered_set<txn_id_t> granted_txn;
        for (const auto &lock_request : it.second->request_queue_) {
          if (lock_request->granted_) {
            granted_txn.insert(lock_request->txn_id_);
          } else {
            for (const auto &txn_id : granted_txn) {
              map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        it.second->latch_.unlock();
      }

      for (const auto &it : row_lock_map_) {
        it.second->latch_.lock();
        std::unordered_set<txn_id_t> granted_txn;
        for (const auto &lock_request : it.second->request_queue_) {
          if (lock_request->granted_) {
            granted_txn.insert(lock_request->txn_id_);
          } else {
            for (const auto &txn_id : granted_txn) {
              map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        it.second->latch_.unlock();
      }

      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        DeleteTxn(txn_id);
        if (map_txn_oid_.count(txn_id) > 0) {
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.lock();
          table_lock_map_[map_txn_oid_[txn_id]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.unlock();
        }
        if (map_txn_rid_.count(txn_id) > 0) {
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.lock();
          row_lock_map_[map_txn_rid_[txn_id]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.unlock();
        }
      }
      waits_for_.clear();
      txn_set_.clear();
      safe_set_.clear();
      active_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

void LockManager::DeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
  }
}

void LockManager::InsertTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (const auto &it : lock_request_queue->request_queue_) {
    if (it->granted_) {
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (it->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              it->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || it->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (it->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (it->lock_mode_ == LockMode::SHARED || it->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              it->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (it->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || it->lock_mode_ == LockMode::SHARED ||
              it->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || it->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
      }
    } else if (it != lock_request) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}

void LockManager::DeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED: {
      auto table_set = txn->GetSharedRowLockSet()->find(lock_request->oid_);
      if (table_set == txn->GetSharedRowLockSet()->end()) {
        return;
      }
      table_set->second.erase(lock_request->rid_);
      break;
    }
    case LockMode::EXCLUSIVE: {
      auto table_set = txn->GetExclusiveRowLockSet()->find(lock_request->oid_);
      if (table_set == txn->GetExclusiveRowLockSet()->end()) {
        return;
      }
      table_set->second.erase(lock_request->rid_);
      break;
    }
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}
void LockManager::InsertRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED: {
      auto table_set = txn->GetSharedRowLockSet()->find(lock_request->oid_);
      if (table_set == txn->GetSharedRowLockSet()->end()) {
        txn->GetSharedRowLockSet()->emplace(lock_request->oid_, std::unordered_set<RID>{});
        table_set = txn->GetSharedRowLockSet()->find(lock_request->oid_);
      }
      table_set->second.insert(lock_request->rid_);
      break;
    }
    case LockMode::EXCLUSIVE: {
      auto table_set = txn->GetExclusiveRowLockSet()->find(lock_request->oid_);
      if (table_set == txn->GetExclusiveRowLockSet()->end()) {
        txn->GetExclusiveRowLockSet()->emplace(lock_request->oid_, std::unordered_set<RID>{});
        table_set = txn->GetExclusiveRowLockSet()->find(lock_request->oid_);
      }
      table_set->second.insert(lock_request->rid_);
      break;
    }
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

}  // namespace bustub
