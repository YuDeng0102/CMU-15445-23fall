#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      heap_([&](const PTR &ua, const PTR &ub) {
        const Tuple &a = ua.first;
        const Tuple &b = ub.first;
        for (const auto &[type, expr] : plan_->GetOrderBy()) {
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
      }) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  if (values_.empty()) {
    Tuple tp;
    RID rd;
    while (child_executor_->Next(&tp, &rd)) {
      heap_.push({tp, rd});
      if (heap_.size() > plan_->n_) {
        heap_.pop();
      }
    }
    while (!heap_.empty()) {
      values_.emplace_back(heap_.top());
      heap_.pop();
    }
    std::reverse(values_.begin(), values_.end());
  }
  cursor_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= values_.size()) {
    return false;
  }
  *tuple = values_[cursor_].first;
  *rid = values_[cursor_].second;
  ++cursor_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); };

}  // namespace bustub
