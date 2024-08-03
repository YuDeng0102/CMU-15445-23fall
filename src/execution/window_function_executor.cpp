#include "execution/executors/window_function_executor.h"
#include <sys/types.h>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include "buffer/clock_replacer.h"

#include "execution/plans/window_plan.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  std::vector<Tuple> tuples;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.emplace_back(tuple);
  }
  // step1:sort
  auto &order_by = plan_->window_functions_.begin()->second.order_by_;
  if (!order_by.empty()) {
    std::sort(tuples.begin(), tuples.end(), [&](const Tuple &a, const Tuple &b) {
      for (const auto &[type, expr] : order_by) {
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
  auto &window_functions = plan_->window_functions_;
  auto &columns = plan_->columns_;
  for (uint32_t i = 0; i < columns.size(); i++) {
    if (window_functions.count(i) != 0U) {
      hts_.emplace_back(window_functions.at(i).type_);
    } else {
      hts_.emplace_back(WindowFunctionType::CountStarAggregate);
    }
  }
  // step2 insert into hashtable
  std::unordered_map<AggregateKey, std::vector<Tuple>> bucket;
  std::vector keys(tuples.size(), std::vector<AggregateKey>(columns.size()));
  for (uint32_t j = 0; j < tuples.size(); j++) {
    auto &tp = tuples[j];
    std::vector<Value> values;
    for (uint32_t i = 0; i < columns.size(); i++) {
      auto &ht = hts_[i];
      if (window_functions.count(i) == 0U) {
        values.emplace_back(columns[i]->Evaluate(&tp, child_executor_->GetOutputSchema()));
      } else {
        Value new_val;
        if (window_functions.at(i).type_ == WindowFunctionType::Rank) {
          new_val = order_by[0].second->Evaluate(&tp, child_executor_->GetOutputSchema());
        } else {
          new_val = window_functions.at(i).function_->Evaluate(&tp, child_executor_->GetOutputSchema());
        }
        auto key = MakeWindowKey(&tp, window_functions.at(i));
        keys[j][i] = key;
        ht.InsertCombine(key, new_val);
        values.emplace_back(ht.Get(key));
      }
    }
    tuples_.emplace_back(values);
  }

  // step3 deal with no order by
  for (uint32_t j = 0; j < tuples_.size(); j++) {
    for (uint32_t i = 0; i < columns.size(); i++) {
      if (window_functions.count(i) != 0U && window_functions.at(i).order_by_.empty()) {
        tuples_[j][i] = hts_[i].Get(keys[j][i]);
      }
    }
  }
}
auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  *tuple = Tuple(tuples_.front(), &GetOutputSchema());
  *rid = tuple->GetRid();
  tuples_.pop_front();
  return true;
}
}  // namespace bustub
