#include <memory>
#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto predicate = dynamic_cast<ComparisonExpression *>(seq_plan.filter_predicate_.get());
    if (predicate != nullptr) {
      if (predicate->comp_type_ == ComparisonType::Equal) {
        auto table_name = seq_plan.table_name_;
        TableInfo *table_info = catalog_.GetTable(table_name);
        auto colum_expr = dynamic_cast<ColumnValueExpression *>(predicate->GetChildAt(0).get());
        for (auto index_info : catalog_.GetTableIndexes(table_name)) {
          auto &index_keys = index_info->key_schema_;
          if (index_keys.GetColumnCount() == 1 &&
              index_keys.GetColumn(0).GetName() == table_info->schema_.GetColumn(colum_expr->GetColIdx()).GetName()) {
            auto pred_key = dynamic_cast<const ConstantValueExpression *>(predicate->GetChildAt(1).get());
            if (pred_key != nullptr) {
              return std::make_shared<IndexScanPlanNode>(
                  seq_plan.output_schema_, seq_plan.table_oid_, index_info->index_oid_,
                  std::dynamic_pointer_cast<ComparisonExpression>(seq_plan.filter_predicate_),
                  const_cast<ConstantValueExpression *>(pred_key));
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
