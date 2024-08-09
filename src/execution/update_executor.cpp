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
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
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
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexs = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // BUSTUB_ASSERT(indexs.size() == 1U && indexs[0]->is_primary_key_, "you should only have primary key!");
  primary_index_ = indexs.empty() ? nullptr : indexs[0];
  Tuple child_tuple{};
  RID rid;
  auto table_schema = child_executor_->GetOutputSchema();
  std::vector<Value> values{};
  values.reserve(table_schema.GetColumnCount());
  while (child_executor_->Next(&child_tuple, &rid)) {
    old_tuples_.emplace_back(rid, std::move(child_tuple));
    values.clear();
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuples_.back().second, table_schema));
    }
    target_tuples_.emplace_back(values, &table_schema);
    if (primary_index_ != nullptr) {
      DeleteTuple(table_info_, txn_mgr_, txn_, table_info_->table_->GetTuple(rid).second,
                  table_info_->table_->GetTupleMeta(rid), &rid);
    }
  }
  reverse(old_tuples_.begin(), old_tuples_.end());
  reverse(target_tuples_.begin(), target_tuples_.end());
}
auto UpdateExecutor::CheckPrimaryKeyUpdated(const Tuple &new_tuple, const Tuple &old_tuple, Schema *schema,
                                            IndexInfo *primary_index) -> bool {
  if (primary_index == nullptr) {
    return false;
  }
  auto key_idx = primary_index->index_->GetKeyAttrs()[0];
  return !new_tuple.GetValue(schema, key_idx).CompareExactlyEquals(old_tuple.GetValue(schema, key_idx));
}
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  is_done_ = true;
  int update_cnt = 0;
  while (!old_tuples_.empty()) {
    auto val = old_tuples_.back();
    *rid = val.first;
    Tuple target_tuple = target_tuples_.back();
    target_tuples_.pop_back();
    old_tuples_.pop_back();
    auto table_schema = child_executor_->GetOutputSchema();
    const auto &[current_meta, current_tuple] = table_info_->table_->GetTuple(*rid);
    if (primary_index_ != nullptr) {
      InsertTuple(primary_index_, table_info_, txn_mgr_, txn_, &target_tuple);
    } else {
      auto version_link = txn_mgr_->GetVersionLink(*rid);
      if (table_info_->table_->GetTupleMeta(*rid).ts_ != txn_->GetTransactionId()) {
        // generate new undolog
        LockAndCheck(txn_mgr_, *rid, txn_, table_info_);
        UndoLog undo_log = GenerateDiffLog(current_tuple, current_meta, target_tuple,
                                           TupleMeta{txn_->GetTransactionTempTs(), false}, &table_schema);
        if (version_link.has_value()) {
          undo_log.prev_version_ = version_link->prev_;
        }
        VersionUndoLink new_version_link;
        new_version_link.prev_ = txn_->AppendUndoLog(undo_log);
        txn_mgr_->UpdateVersionLink(*rid, new_version_link);
        txn_->AppendWriteSet(table_info_->oid_, *rid);
      } else if (version_link.has_value() && version_link->prev_.IsValid()) {
        UndoLog undo_log = GenerateDiffLog(current_tuple, current_meta, target_tuple,
                                           TupleMeta{txn_->GetTransactionTempTs(), false}, &table_schema);
        UndoLog old_undo_log = txn_->GetUndoLog(version_link->prev_.prev_log_idx_);
        txn_->ModifyUndoLog(version_link->prev_.prev_log_idx_, MergeUndolog(old_undo_log, undo_log, &table_schema));
      }
      table_info_->table_->UpdateTupleInPlace({txn_->GetTransactionTempTs(), false}, target_tuple, *rid);
    }
    ++update_cnt;
  }
  std::vector<Value> result_values = {{TypeId::INTEGER, update_cnt}};
  *tuple = Tuple(result_values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
