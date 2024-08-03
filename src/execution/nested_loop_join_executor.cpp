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
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/plans/abstract_plan.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  predicate_ = plan_->Predicate();
  left_state_ = left_executor_->Next(&left_tuple_, &left_rid_);
  is_find_ = false;
  // Tuple right_tuple;
  // RID right_rid;
  // while(right_executor_->Next(&right_tuple, &right_rid)) {
  //   right_tuples_.emplace_back(right_tuple);
  // }
  // curosr_=0;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  while (left_state_) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // Tuple& right_tuple=right_tuples_[curosr_++];
      auto res = predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                          right_executor_->GetOutputSchema());
      if (!res.IsNull() && res.GetAs<bool>()) {
        is_find_ = true;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = {values, &GetOutputSchema()};
        return true;
      }
    }
    if (!is_find_) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        is_find_ = true;
        *tuple = {values, &GetOutputSchema()};
        return true;
      }
    }
    left_state_ = left_executor_->Next(&left_tuple_, &left_rid_);
    right_executor_->Init();
    is_find_ = false;
  }
  return false;
}

}  // namespace bustub
