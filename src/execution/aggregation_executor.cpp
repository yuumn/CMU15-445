//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid{};
  while (child_->Next(&tuple, &rid)) {
    // std::cout<<"**"<<'\n';
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout<<"agg\n";
  if (aht_iterator_ == aht_.End()) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }
    if (aht_.Empty() && !is_agg_) {
      AggregateValue values = aht_.GenerateInitialAggregateValue();
      *tuple = Tuple(values.aggregates_, &plan_->OutputSchema());
      *rid = tuple->GetRid();
      is_agg_ = true;
      return true;
    }
    return false;
  }
  auto key = aht_iterator_.Key();
  auto value = aht_iterator_.Val();
  std::vector<Value> values;
  for (const auto &it : key.group_bys_) {
    values.push_back(it);
  }
  for (const auto &it : value.aggregates_) {
    values.push_back(it);
  }
  *tuple = Tuple(values, &plan_->OutputSchema());
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
