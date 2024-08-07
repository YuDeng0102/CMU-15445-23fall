//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <vector>
#include "type/value.h"


namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_((plan)) {}

void IndexScanExecutor::Init() { is_done_ = false; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  is_done_ = true;
  auto catalog = this->exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->index_oid_);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  auto txn = this->exec_ctx_->GetTransaction();
  auto txn_manager = this->exec_ctx_->GetTransactionManager();
  auto ts = txn->GetReadTs();
  std::vector<RID> result;
  auto key = Tuple(std::vector<Value>({plan_->pred_key_->val_}), &index_info->key_schema_);
  htable->ScanKey(key, &result, this->exec_ctx_->GetTransaction());
  if (result.empty()) {
    return false;
  }
  *rid = result[0];

  auto table_info = catalog->GetTable(plan_->table_oid_);
  const auto [base_meta,base_tuple] =table_info->table_->GetTuple(*rid);
   if (base_meta.ts_ <= ts || txn->GetTransactionId() == base_meta.ts_) {
      if (!base_meta.is_deleted_) {
        *tuple=base_tuple;
      }
      return !base_meta.is_deleted_;
    }
    auto undo_link = txn_manager->GetUndoLink(base_tuple.GetRid());
    if (undo_link == std::nullopt) {
      return false;
    }
    std::vector<UndoLog> undo_logs;

    while (undo_link->IsValid()) {
      std::unique_lock<std::shared_mutex> l(txn_manager->txn_map_mutex_);
      auto tem_txn = txn_manager->txn_map_[undo_link->prev_txn_];
      const auto &undo_log = tem_txn->GetUndoLog(undo_link->prev_log_idx_);
      l.unlock();

      if (undo_log.ts_ <= ts || undo_log.ts_ == txn->GetTransactionId()) {
        undo_logs.emplace_back(undo_log);
        break;
      }
      undo_link = undo_log.prev_version_;
    }

    if (undo_logs.empty() || undo_logs.rbegin()->is_deleted_ || undo_logs.rbegin()->ts_ > ts) {
      return false;
    }
    auto res= ReconstructTuple(&GetOutputSchema(), base_tuple, base_meta, undo_logs);
    if(res.has_value()){
      *tuple=*res;
    }
    return true;
}

}  // namespace bustub
