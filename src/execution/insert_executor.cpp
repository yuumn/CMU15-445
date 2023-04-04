//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    bool get_lock = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!get_lock) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_insert_) {
    return false;
  }
  is_insert_ = true;

  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);
  int cnt = 0;

  while (child_executor_->Next(tuple, rid)) {
    bool is_insert = table->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    if (is_insert) {
      try {
        bool get_lock = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                             LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
        if (!get_lock) {
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }

      for (auto index : indexs) {
        Tuple index_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                index->index_->GetMetadata()->GetKeyAttrs());
        index->index_->InsertEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
      }
      cnt++;
    }
  }

  std::vector<Value> ans{Value(INTEGER, cnt)};
  *tuple = Tuple(ans, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
