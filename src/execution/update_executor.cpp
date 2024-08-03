//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include "storage/table/tuple.h"
#include "type/type_id.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_((plan)), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  is_done_ = true;
  int update_cnt = 0;

  Tuple child_tuple{};

  // Get the next tuple
  while (child_executor_->Next(&child_tuple, rid)) {
    auto indexs = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    // Compute expressions
    std::vector<Value> values{};
    auto table_scheme = table_info_->schema_;
    values.reserve(table_scheme.GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, table_scheme));
    }
    *tuple = Tuple{values, &table_scheme};
    // Insert record
    RID new_rid = table_info_->table_->InsertTuple(TupleMeta(), *tuple).value();

    for (auto index : indexs) {
      auto key_tuple = child_tuple.KeyFromTuple(table_scheme, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, *rid, this->exec_ctx_->GetTransaction());

      key_tuple = tuple->KeyFromTuple(table_scheme, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, new_rid, this->exec_ctx_->GetTransaction());
    }

    ++update_cnt;
  }

  std::vector<Value> result_values = {{TypeId::INTEGER, update_cnt}};
  *tuple = Tuple(result_values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
