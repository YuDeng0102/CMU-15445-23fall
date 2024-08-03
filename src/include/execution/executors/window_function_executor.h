//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bits/types/wint_t.h>
#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/boolean_type.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunTable {
 public:
  explicit WindowFunTable(const WindowFunctionType &window_function_type)
      : window_function_type_(window_function_type) {}

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialValue() -> Value {
    Value value;
    switch (window_function_type_) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        value = ValueFactory::GetZeroValueByType(TypeId::INTEGER);
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // Others starts at null.
        value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
      case WindowFunctionType::Rank:
        value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
    }
    return value;
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineValue(Value *result, const Value &input) {
    if (window_function_type_ != WindowFunctionType::CountStarAggregate && input.IsNull()) {
      return;
    }
    auto &res = *result;
    switch (window_function_type_) {
      case WindowFunctionType::CountStarAggregate:
        res = res.Add(Value(TypeId::INTEGER, 1));
        break;

      case WindowFunctionType::CountAggregate:
        if (res.IsNull()) {
          res = Value(TypeId::INTEGER, 1);
        } else {
          res = res.Add(Value(TypeId::INTEGER, 1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (res.IsNull()) {
          res = Value(TypeId::INTEGER, 0);
        }
        res = res.Add(input);
        break;
      case WindowFunctionType::MinAggregate:
        if (res.IsNull()) {
          res = Value(input);
        } else {
          res = res.CompareGreaterThan(input) == CmpBool::CmpTrue ? input : res;
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (res.IsNull()) {
          res = Value(input);
        } else {
          res = res.CompareGreaterThan(input) == CmpBool::CmpTrue ? res : input;
        }
        break;
      case WindowFunctionType::Rank:
        rank_++;
        if (old_value_.IsNull() || old_value_.CompareEquals(input) == CmpBool::CmpFalse) {
          res = Value(TypeId::INTEGER, rank_);
        }

        old_value_ = input;
        break;
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const Value &val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialValue()});
    }
    CombineValue(&ht_[agg_key], val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  auto Get(const AggregateKey &agg_key) -> Value {
    auto &result = ht_.at(agg_key);
    return result;
  }

 private:
  std::unordered_map<AggregateKey, Value> ht_{};
  WindowFunctionType window_function_type_;
  Value old_value_;
  int rank_{0};
};

class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  auto MakeWindowKey(const Tuple *tuple, const WindowFunctionPlanNode::WindowFunction &windoe_function)
      -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : windoe_function.partition_by_) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<WindowFunTable> hts_;
  std::deque<std::vector<Value>> tuples_;
};
}  // namespace bustub
