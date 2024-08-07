//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <mutex>
#include <shared_mutex>
#include <utility>
#include "catalog/catalog.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {
  // iter_=std::move(this->GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
  auto txn = this->exec_ctx_->GetTransaction();
  auto txn_manager = this->exec_ctx_->GetTransactionManager();
  auto ts = txn->GetReadTs();
  while (!iter_.IsEnd()) {
    const auto &[base_meta, base_tuple] = iter_.GetTuple();
    ++iter_;
    if (base_meta.ts_ <= ts || txn->GetTransactionId() == base_meta.ts_) {
      if (!base_meta.is_deleted_) {
        val_.emplace_back(base_tuple);
      }
      continue;
    }
    auto undo_link = txn_manager->GetUndoLink(base_tuple.GetRid());
    if (undo_link == std::nullopt) {
      continue;
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
      continue;
    }
    auto tuple = ReconstructTuple(&GetOutputSchema(), base_tuple, base_meta, undo_logs);
    if (tuple.has_value()) {
      val_.emplace_back(*tuple);
    }
  }
  viter_ = val_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (viter_ == val_.end()) {
    return false;
  }
  auto filter_predicate = plan_->filter_predicate_;
  auto tp = *viter_;

  auto check = [&](Tuple *tp) -> bool {
    if (filter_predicate == nullptr) {
      return true;
    }
    auto val = filter_predicate->Evaluate(tp, GetOutputSchema());
    return !val.IsNull() && val.GetAs<bool>();
  };
  while (!check(&tp)) {
    ++viter_;
    if (viter_ == val_.end()) {
      return false;
    }
    auto k = *viter_;
    tp = std::move(k);
    if (check(&tp)) {
      break;
    }
  }
  *tuple = std::move(tp);
  *rid = tuple->GetRid();
  ++viter_;
  return true;
}

}  // namespace bustub
