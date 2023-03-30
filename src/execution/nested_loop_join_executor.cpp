//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
  right_tuples_size_ = right_tuples_.size();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout<<"LoopJoin"<<'\n';
  if (right_tuples_index_ >= 0 && right_tuples_index_ < right_tuples_size_) {
    for (int i = right_tuples_index_; i < right_tuples_size_; i++) {
      if (IsMatch(&left_tuple_, &right_tuples_[i])) {
        std::vector<Value> values;
        for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
        }
        for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(right_tuples_[i].GetValue(&right_executor_->GetOutputSchema(), j));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        right_tuples_index_ = i + 1;
        return true;
      }
    }
  }
  while (left_executor_->Next(&left_tuple_, rid)) {
    for (int i = 0; i < right_tuples_size_; i++) {
      if (IsMatch(&left_tuple_, &right_tuples_[i])) {
        std::vector<Value> values;
        for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
        }
        for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(right_tuples_[i].GetValue(&right_executor_->GetOutputSchema(), j));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        right_tuples_index_ = i + 1;
        return true;
      }
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
      }
      for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      right_tuples_index_ = -1;
      return true;
    }
  }
  return false;
}
auto NestedLoopJoinExecutor::IsMatch(Tuple *left_tuple, Tuple *right_tuple) -> bool {
  auto value = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                               right_executor_->GetOutputSchema());
  return !value.IsNull() && value.GetAs<bool>();
}

}  // namespace bustub
