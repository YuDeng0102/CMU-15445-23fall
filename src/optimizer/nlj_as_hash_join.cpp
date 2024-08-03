#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void ParseAndExpression(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &left,
                        std::vector<AbstractExpressionRef> &right) {
  auto logical_expression = dynamic_cast<const LogicExpression *>(predicate.get());
  if (logical_expression != nullptr) {
    ParseAndExpression(predicate->GetChildAt(0), left, right);
    ParseAndExpression(predicate->GetChildAt(1), left, right);
  }
  auto comparison_expression = dynamic_cast<const ComparisonExpression *>(predicate.get());
  if (comparison_expression != nullptr) {
    auto val_1 = dynamic_cast<ColumnValueExpression *>(comparison_expression->GetChildAt(0).get());
    ;
    if (val_1->GetTupleIdx() == 0) {
      left.emplace_back(comparison_expression->GetChildAt(0));
      right.emplace_back(comparison_expression->GetChildAt(1));
    } else {
      left.emplace_back(comparison_expression->GetChildAt(1));
      right.emplace_back(comparison_expression->GetChildAt(0));
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    auto predicate = nlj_plan.predicate_;
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    ParseAndExpression(predicate, left_key_expressions, right_key_expressions);
    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_key_expressions, right_key_expressions, nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

}  // namespace bustub
