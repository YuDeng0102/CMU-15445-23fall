//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "type/type_id.h"
#include "type/value.h"

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }

  int delete_cnt = 0;
  auto catalog = this->exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto indexs = catalog->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  while (child_executor_->Next(tuple, rid)) {
    // check  w-w conflict
    if (IsWriteWriteConflict(rid, table_info, txn)) {
      txn->SetTainted();
      throw ExecutionException("w-w conflict");
    }
    const auto &origin_meta = table_info->table_->GetTupleMeta(*rid);
    auto table_scheme = table_info->schema_;
    auto sz = table_scheme.GetColumnCount();

    auto version_link = txn_mgr->GetVersionLink(*rid);
    if (table_info->table_->GetTupleMeta(*rid).ts_ != txn->GetTransactionId()) {
      // generate new undolog

      std::vector<bool> modified_field(sz, true);
      std::vector<Value> undo_values;
      for (uint32_t i = 0; i < sz; i++) {
        undo_values.emplace_back(tuple->GetValue(&table_scheme, i));
      }
      UndoLog undo_log({false, std::move(modified_field), Tuple({undo_values, &table_scheme}), origin_meta.ts_});
      if (version_link.has_value()) {
        undo_log.prev_version_ = version_link->prev_;
      }
      VersionUndoLink new_version_link;
      new_version_link.prev_ = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateVersionLink(*rid, new_version_link);
      txn->AppendWriteSet(table_info->oid_, *rid);
    } else if (version_link.has_value() && version_link->prev_.IsValid()) {
      auto undolog = txn->GetUndoLog(version_link->prev_.prev_log_idx_);
      std::vector<Value> new_values;
      for (uint32_t i = 0; i < sz; i++) {
        if (!undolog.modified_fields_[i]) {
          undolog.modified_fields_[i] = true;
          new_values.emplace_back(tuple->GetValue(&table_scheme, i));
        } else {
          new_values.emplace_back(undolog.tuple_.GetValue(&table_scheme, i));
        }
      }
      undolog.tuple_ = Tuple({new_values, &table_scheme});
      txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, undolog);
    }

    table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, *rid);
    delete_cnt++;
  }
  std::vector<Value> values;

  values.emplace_back(TypeId::INTEGER, delete_cnt);
  *tuple = Tuple(values, &GetOutputSchema());
  is_done_ = true;
  return true;
}

}  // namespace bustub
