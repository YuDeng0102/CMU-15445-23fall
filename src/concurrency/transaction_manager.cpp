//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  std::lock_guard<std::mutex> commit_lck(commit_mutex_);
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  // std::unique_lock<std::mutex> lck(commit_mutex_);
  txn->commit_ts_ = last_commit_ts_.load() + 1;
  for (auto &[table_oid, s] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    for (auto &rid : s) {
      auto meta = table_info->table_->GetTupleMeta(rid);
      meta.ts_ = txn->commit_ts_;
      table_info->table_->UpdateTupleMeta(meta, rid);
    }
  }
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  ++last_commit_ts_;
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  std::vector<txn_id_t> remove_tx;
  std::unordered_set<RID> visited_records;
  std::unordered_map<txn_id_t, uint32_t> txn_free_cnt;
  auto ts = GetWatermark();
  for (const auto &[tx_id, txn] : txn_map_) {
    if (txn->state_ != TransactionState::COMMITTED && txn->state_ != TransactionState::ABORTED) {
      continue;
    }

    if (txn->GetUndoLogNum() == 0U) {
      remove_tx.emplace_back(tx_id);
    }
    for (auto &[oid, writeset] : txn->GetWriteSets()) {
      for (auto &rid : writeset) {
        if (visited_records.count(rid)) {
          continue;
        }
        visited_records.insert(rid);
        bool ok = catalog_->GetTable(oid)->table_->GetTupleMeta(rid).ts_ <= ts;
        auto head = GetVersionLink(rid);
        if (head.has_value()) {
          auto undo_link = head->prev_;
          while (undo_link.IsValid()) {
            if (!txn_map_.count(undo_link.prev_txn_)) {
              break;
            }
            const auto next_txn = txn_map_.at(undo_link.prev_txn_);
            const auto undo_log = next_txn->GetUndoLog(undo_link.prev_log_idx_);
            if (undo_log.ts_ < ts && ok) {
              txn_free_cnt[undo_link.prev_txn_]++;
              if (txn_free_cnt[undo_link.prev_txn_] == next_txn->GetUndoLogNum()) {
                remove_tx.emplace_back(undo_link.prev_txn_);
              }
            }
            ok |= undo_log.ts_ <= ts;
            undo_link = undo_log.prev_version_;
          }
        }
      }
    }
  }
  for (auto txn_id : remove_tx) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
