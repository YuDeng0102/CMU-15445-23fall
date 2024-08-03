#include <memory>
#include <vector>
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &u : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(u));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto child = limit.GetChildAt(0);
    if (child->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child);
      return std::make_shared<TopNPlanNode>(sort_plan.output_schema_, sort_plan.GetChildAt(0), sort_plan.GetOrderBy(),
                                            limit.GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
