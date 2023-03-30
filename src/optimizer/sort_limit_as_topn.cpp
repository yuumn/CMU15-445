#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // return plan;
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit && optimized_plan->GetChildren().size() == 1 &&
      optimized_plan->GetChildren()[0]->GetType() == PlanType::Sort) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*optimized_plan->GetChildren()[0]);
    SchemaRef schema = std::make_shared<const Schema>(limit_plan.OutputSchema());
    AbstractPlanNodeRef child = sort_plan.GetChildPlan();
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys = sort_plan.GetOrderBy();
    std::size_t n = limit_plan.GetLimit();
    return std::make_shared<TopNPlanNode>(schema, child, order_bys, n);
  }
  return optimized_plan;
}

}  // namespace bustub
