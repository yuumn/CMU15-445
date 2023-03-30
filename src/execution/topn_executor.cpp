#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
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
  child_tuples_index_ = 0;
  cnt_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout<<"topn\n";
  if (cnt_ >= std::min(plan_->GetN(), child_tuples_.size())) {
    return false;
  }
  *tuple = child_tuples_[child_tuples_index_];
  *rid = tuple->GetRid();
  child_tuples_index_++;
  cnt_++;
  return true;
}

}  // namespace bustub
