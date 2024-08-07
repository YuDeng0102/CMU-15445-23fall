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
#include "execution/executors/update_executor.h"
#include <memory>
#include <optional>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
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
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  // Get the next tuple
  while (child_executor_->Next(&child_tuple, rid)) {
    // check  w-w conflict
    if (IsWriteWriteConflict(rid, table_info_, txn)) {
      txn->SetTainted();
      throw ExecutionException("w-w conflict");
    }

    // Compute expressions
    std::vector<Value> values{};
    auto table_scheme = table_info_->schema_;
    auto sz = table_scheme.GetColumnCount();

    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, table_scheme));
    }
    *tuple = Tuple{values, &table_scheme};
    const auto &origin_meta = table_info_->table_->GetTupleMeta(*rid);
    auto version_link = txn_mgr->GetVersionLink(*rid);
    if (table_info_->table_->GetTupleMeta(*rid).ts_ != txn->GetTransactionId()) {
      // generate new undolog

      std::vector<bool> modified_field(sz);
      std::vector<Value> undo_values;
      for (uint32_t i = 0; i < sz; i++) {
        if (!values[i].CompareExactlyEquals(child_tuple.GetValue(&table_scheme, i))) {
          modified_field[i] = true;
          undo_values.emplace_back(child_tuple.GetValue(&table_scheme, i));
        }
      }
      auto subschema = GetSubSchema(modified_field, &table_scheme);
      UndoLog undo_log({false, std::move(modified_field), Tuple({undo_values, &subschema}), origin_meta.ts_});
      if (version_link.has_value()) {
        undo_log.prev_version_ = version_link->prev_;
      }
      VersionUndoLink new_version_link;
      new_version_link.prev_ = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateVersionLink(*rid, new_version_link);
      txn->AppendWriteSet(table_info_->oid_, *rid);
    } else if (version_link.has_value() && version_link->prev_.IsValid()) {
      auto undolog = txn->GetUndoLog(version_link->prev_.prev_log_idx_);
      std::vector<Value> new_values;
      Schema undolog_schema = GetSubSchema(undolog, &table_scheme);
      for (uint32_t i = 0, j = 0; i < sz; i++) {
        bool eqs = values[i].CompareExactlyEquals(child_tuple.GetValue(&table_scheme, i));
        if (!eqs && !undolog.modified_fields_[i]) {
          undolog.modified_fields_[i] = true;
          new_values.emplace_back(child_tuple.GetValue(&table_scheme, i));
        } else if (undolog.modified_fields_[i]) {
          new_values.emplace_back(undolog.tuple_.GetValue(&undolog_schema, j));
          ++j;
        }
      }
      auto subschema = GetSubSchema(undolog.modified_fields_, &table_scheme);
      undolog.tuple_ = Tuple({new_values, &subschema});
      txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, undolog);
    }

    Tuple tp(values, &table_scheme);
    table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, tp, *rid);

    ++update_cnt;
  }

  std::vector<Value> result_values = {{TypeId::INTEGER, update_cnt}};
  *tuple = Tuple(result_values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
