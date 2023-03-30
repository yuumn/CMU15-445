//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      index_info_{exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!rids_.empty()) {
    Tuple right_tuple;
    table_info_->table_->GetTuple(rids_[rids_.size() - 1], &right_tuple, exec_ctx_->GetTransaction());
    rids_.pop_back();
    std::vector<Value> values;
    for (uint32_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j++) {
      values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), j));
    }
    for (uint32_t j = 0; j < plan_->InnerTableSchema().GetColumnCount(); j++) {
      values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), j));
    }
    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }
  // RID rid_;
  while (child_executor_->Next(&left_tuple_, rid)) {
    Value value = plan_->KeyPredicate()->Evaluate(&left_tuple_, child_executor_->GetOutputSchema());
    // std::cout << "left_tuple_:" << value.ToString() << '\n';
    Tuple tuple1 = Tuple({value}, index_info_->index_->GetKeySchema());
    tree_->ScanKey(tuple1, &rids_, exec_ctx_->GetTransaction());
    reverse(rids_.begin(), rids_.end());
    if (!rids_.empty()) {
      Tuple right_tuple;
      table_info_->table_->GetTuple(rids_[rids_.size() - 1], &right_tuple, exec_ctx_->GetTransaction());
      rids_.pop_back();
      std::vector<Value> values;
      for (uint32_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), j));
      }
      for (uint32_t j = 0; j < plan_->InnerTableSchema().GetColumnCount(); j++) {
        values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), j));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), j));
      }
      for (uint32_t j = 0; j < plan_->InnerTableSchema().GetColumnCount(); j++) {
        values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(j).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
