#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    child_tuples_.push_back(tuple);
  }
  std::sort(child_tuples_.begin(), child_tuples_.end(), [this](const Tuple &l, const Tuple &r) {
    auto schema = child_executor_->GetOutputSchema();
    for (auto &[order_by_type, expr] : plan_->GetOrderBy()) {
      switch (order_by_type) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(expr->Evaluate(&l, schema).CompareLessThan(expr->Evaluate(&r, schema)))) {
            return true;
          } else if (static_cast<bool>(expr->Evaluate(&l, schema).CompareGreaterThan(expr->Evaluate(&r, schema)))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(expr->Evaluate(&l, schema).CompareLessThan(expr->Evaluate(&r, schema)))) {
            return false;
          } else if (static_cast<bool>(expr->Evaluate(&l, schema).CompareGreaterThan(expr->Evaluate(&r, schema)))) {
            return true;
          }
          break;
      }
    }
    return false;
  });
  child_tuples_size_ = child_tuples_.size();
  child_tuples_index_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_tuples_index_ == child_tuples_size_) {
    return false;
  }
  *tuple = child_tuples_[child_tuples_index_];
  *rid = tuple->GetRid();
  child_tuples_index_++;
  return true;
}

}  // namespace bustub
