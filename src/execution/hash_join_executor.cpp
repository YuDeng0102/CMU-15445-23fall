//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <memory>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      ht_(nullptr) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;
  if (ht_ == nullptr) {
    ht_ = std::make_unique<HashJoinHashTable>();
    while (right_child_->Next(&tuple, &rid)) {
      auto key = GetRightKey(&tuple);
      ht_->Insert(key, tuple);
    }
  }
  last_vec_ = nullptr;
  left_state_ = left_child_->Next(&left_tuple_, &rid);
  is_find_ = false;
}
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;
  Tuple right_tuple;
  while (left_state_) {
    auto key = GetLeftKey(&left_tuple_);
    auto vec = ht_->Get(key);
    if (vec != nullptr) {
      if (is_find_) {
        if (last_iter_ == prev(vec->end())) {
          left_state_ = left_child_->Next(&left_tuple_, rid);
          is_find_ = false;
          last_vec_ = nullptr;
          continue;
        }
        last_iter_++;
      } else {
        is_find_ = true;
        last_iter_ = vec->begin();
        last_vec_ = vec;
      }
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      right_tuple = *last_iter_;
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }
      *tuple = {values, &GetOutputSchema()};
      return true;
    }

    if (!is_find_ && plan_->join_type_ == JoinType::LEFT) {
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = {values, &GetOutputSchema()};
      left_state_ = left_child_->Next(&left_tuple_, rid);
      last_vec_ = nullptr;
      return true;
    }
    left_state_ = left_child_->Next(&left_tuple_, rid);
    last_vec_ = nullptr;
    is_find_ = false;
  }
  return false;
}

}  // namespace bustub
