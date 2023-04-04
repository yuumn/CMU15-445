//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Delete Executor Get Table Lock Failed");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_delete_) {
    return false;
  }
  is_delete_ = true;

  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);
  int cnt = 0;

  while (child_executor_->Next(tuple, rid)) {
    try {
      bool get_lock = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                           LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
      if (!get_lock) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Insert Executor Get Row Lock Failed" + e.GetInfo());
    }

    bool is_delete = table->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (is_delete) {
      for (auto index : indexs) {
        Tuple index_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                index->index_->GetMetadata()->GetKeyAttrs());
        index->index_->DeleteEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
      }
      cnt++;
    }
  }

  std::vector<Value> ans{Value(INTEGER, cnt)};
  *tuple = Tuple(ans, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
