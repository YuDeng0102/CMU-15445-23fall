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
#include "storage/table/tuple.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (!is_inited_ && child_executor_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, val);
  }
  is_inited_ = true;
  is_done_ = false;
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // empty table
  if (aht_.End() == aht_.Begin()) {
    if (is_done_) {
      return false;
    }
    is_done_ = true;
    if (plan_->GetGroupBys().empty()) {
      std::vector<Value> values;
      auto att_vals = aht_.GenerateInitialAggregateValue();
      values = std::move(att_vals.aggregates_);
      *tuple = {values, &GetOutputSchema()};
      return true;
    }
    return false;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;
  for (const auto &key : aht_iterator_.Key().group_bys_) {
    values.emplace_back(key);
  }
  for (const auto &val : aht_iterator_.Val().aggregates_) {
    values.emplace_back(val);
  }
  *tuple = {values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
