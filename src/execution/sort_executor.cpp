#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  if (!sorted_) {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      values_.emplace_back(tuple, rid);
    }

    sort(values_.begin(), values_.end(), [&](auto &ua, auto &ub) {
      for (const auto &[type, expr] : plan_->GetOrderBy()) {
        Tuple &a = ua.first;
        Tuple &b = ub.first;
        if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
          if (expr->Evaluate(&a, GetOutputSchema()).CompareLessThan(expr->Evaluate(&b, GetOutputSchema())) ==
              CmpBool::CmpTrue) {
            return true;
          }
          if (expr->Evaluate(&a, GetOutputSchema()).CompareGreaterThan(expr->Evaluate(&b, GetOutputSchema())) ==
              CmpBool::CmpTrue) {
            return false;
          }
        } else {
          if (expr->Evaluate(&a, GetOutputSchema()).CompareLessThan(expr->Evaluate(&b, GetOutputSchema())) ==
              CmpBool::CmpTrue) {
            return false;
          }
          if (expr->Evaluate(&a, GetOutputSchema()).CompareGreaterThan(expr->Evaluate(&b, GetOutputSchema())) ==
              CmpBool::CmpTrue) {
            return true;
          }
        }
      }
      return true;
    });
  }
  iter_ = values_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == values_.end()) {
    return false;
  }
  *tuple = iter_->first;
  *rid = iter_->second;
  ++iter_;
  return true;
}

}  // namespace bustub
