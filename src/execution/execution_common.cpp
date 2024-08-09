#include "execution/execution_common.h"
#include <cstdint>
#include <iostream>
#include <optional>
#include <thread>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto GetSubSchema(const UndoLog &undo_log, const Schema *schema) -> Schema {
  std::vector<Column> colums;
  for (uint32_t i = 0; i < undo_log.modified_fields_.size(); i++) {
    if (undo_log.modified_fields_[i]) {
      auto &tem_col = schema->GetColumn(i);
      colums.emplace_back(tem_col.GetName(), tem_col);
    }
  }
  return Schema(colums);
}
auto GetSubSchema(const std::vector<bool> &modified_fields_, const Schema *schema) -> Schema {
  std::vector<Column> colums;
  for (uint32_t i = 0; i < modified_fields_.size(); i++) {
    if (modified_fields_[i]) {
      auto &tem_col = schema->GetColumn(i);
      colums.emplace_back(tem_col.GetName(), tem_col);
    }
  }
  return Schema(colums);
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (base_meta.is_deleted_ && undo_logs.empty()) {
    return std::nullopt;
  }
  std::vector<Value> values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }
  for (auto cnt = 0U; cnt < undo_logs.size(); cnt++) {
    auto &undo_log = undo_logs[cnt];
    if (undo_log.is_deleted_ && cnt == undo_logs.size() - 1) {
      return std::nullopt;
    }
    std::vector<Column> colums;
    std::vector<uint32_t> colum_idx;
    for (uint32_t i = 0; i < undo_log.modified_fields_.size(); i++) {
      if (undo_log.modified_fields_[i]) {
        auto &tem_col = schema->GetColumn(i);
        colums.emplace_back(tem_col.GetName(), tem_col);
        colum_idx.emplace_back(i);
      }
    }
    Schema subschema(colums);
    for (uint32_t i = 0; i < colums.size(); i++) {
      values[colum_idx[i]] = undo_log.tuple_.GetValue(&subschema, i);
    }
  }
  Tuple new_tp(values, schema);
  new_tp.SetRid(base_tuple.GetRid());
  return std::make_optional(new_tp);
}
void SetTxnTainted(Transaction *txn) {
  txn->SetTainted();
  // std::cerr<<"Thread "<<std::this_thread::get_id()<<"SetTainted\n";
  throw ExecutionException("txn is tainted");
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1

  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    const auto &[meta, tuple] = iter.GetTuple();
    auto rid = iter.GetRID();
    ++iter;
    const Schema &schema = table_info->schema_;
    std::string tuple_context;
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      const auto &val = tuple.GetValue(&table_info->schema_, i);
      if (val.IsNull()) {
        tuple_context.append("<NULL>");
      } else {
        tuple_context.append(val.ToString());
      }
      if (i != table_info->schema_.GetColumnCount() - 1) {
        tuple_context.append(", ");
      }
    }

    auto ts_string =
        meta.ts_ >= TXN_START_ID ? "txn" + fmt::to_string(meta.ts_ - TXN_START_ID) : fmt::to_string(meta.ts_);

    fmt::println(stderr, "RID={}/{} ts={}{} tuple=({})", rid.GetPageId(), rid.GetSlotNum(), ts_string,
                 meta.is_deleted_ ? " <delete marker> " : "", tuple_context);
    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (undo_link.has_value()) {
      while (undo_link->IsValid()) {
        std::unique_lock<std::shared_mutex> l(txn_mgr->txn_map_mutex_);
        if (txn_mgr->txn_map_.count(undo_link->prev_txn_) == 0U) {
          break;
        }
        auto tem_txn = txn_mgr->txn_map_[undo_link->prev_txn_];
        const auto &undo_log = tem_txn->GetUndoLog(undo_link->prev_log_idx_);
        l.unlock();

        if (undo_log.is_deleted_) {
          fmt::println(stderr, "\ttxn{} <del> ts={}", tem_txn->GetTransactionId() - TXN_START_ID, tem_txn->GetReadTs());
        } else {
          std::string txn_context;
          Schema subschema = GetSubSchema(undo_log, &schema);
          for (uint32_t i = 0, j = 0; i < schema.GetColumnCount(); i++) {
            if (undo_log.modified_fields_[i]) {
              txn_context += undo_log.tuple_.GetValue(&subschema, j++).ToString();
            } else {
              txn_context += '_';
            }
            if (i != schema.GetColumnCount() - 1) {
              txn_context.append(", ");
            }
          }
          fmt::println(stderr, "\ttxn{} ({}) ts={}", tem_txn->GetTransactionId() - TXN_START_ID, txn_context,
                       tem_txn->GetReadTs());
        }
        undo_link = undo_log.prev_version_;
      }
    }
  }
}
auto IsWriteWriteConflict(RID *rid, const TableInfo *table_info, Transaction *txn) -> bool {
  auto child_meta = table_info->table_->GetTupleMeta(*rid);
  return (child_meta.ts_ > txn->GetReadTs() && child_meta.ts_ != txn->GetTransactionTempTs());
}
auto LockVersionLick(TransactionManager *txn_mgr, RID rid) -> bool {
  std::optional<VersionUndoLink> origin_version_link = txn_mgr->GetVersionLink(rid);
  if (origin_version_link.has_value()) {
    if (origin_version_link->in_progress_) {
      return false;
    }
    return txn_mgr->UpdateVersionLink(
        rid, VersionUndoLink{origin_version_link->prev_, true},
        [origin_version_link](std::optional<VersionUndoLink> current_version_link) -> bool {
          return !current_version_link->in_progress_ && current_version_link->prev_ == origin_version_link->prev_;
        });
  }
  return txn_mgr->UpdateVersionLink(
      rid, VersionUndoLink{{}, true},
      [](std::optional<VersionUndoLink> current_version_link) -> bool { return !current_version_link.has_value(); });
}
void LockAndCheck(TransactionManager *txn_mgr, RID rid, Transaction *txn, const TableInfo *table_info) {
  if (!LockVersionLick(txn_mgr, rid)) {
    SetTxnTainted(txn);
  }
  if (IsWriteWriteConflict(&rid, table_info, txn)) {
    auto version_link_optional = txn_mgr->GetVersionLink(rid);
    txn_mgr->UpdateVersionLink(rid, VersionUndoLink{version_link_optional->prev_, false});
    SetTxnTainted(txn);
  }
}
auto GenerateDiffLog(const Tuple &old_tuple, const TupleMeta &old_meta, const Tuple &new_tuple,
                     const TupleMeta &new_meta, const Schema *schema) -> UndoLog {
  UndoLog undo_log;
  uint32_t sz = schema->GetColumnCount();
  // std::cerr<<old_tuple.ToString(schema)<<" "<<schema->GetColumnCount()<<std::endl;
  undo_log.ts_ = old_meta.ts_;
  if (old_meta.is_deleted_) {
    undo_log.is_deleted_ = true;
    return undo_log;
  }
  undo_log.is_deleted_ = false;
  undo_log.modified_fields_ = std::vector<bool>(sz, false);
  std::vector<Value> undo_values;
  if (new_meta.is_deleted_) {
    for (uint32_t i = 0; i < sz; i++) {
      undo_values.emplace_back(old_tuple.GetValue(schema, i));
      undo_log.modified_fields_[i] = true;
    }
    undo_log.tuple_ = Tuple(undo_values, schema);
  } else {
    for (uint32_t i = 0; i < sz; i++) {
      if (!old_tuple.GetValue(schema, i).CompareExactlyEquals(new_tuple.GetValue(schema, i))) {
        undo_log.modified_fields_[i] = true;
        // std::cerr<<"schema colum size="<<schema->GetColumnCount()<<' '<<old_tuple.ToString(schema)<<'\n';
        undo_values.emplace_back(old_tuple.GetValue(schema, i));
      }
    }
    Schema subschema = GetSubSchema(undo_log.modified_fields_, schema);
    undo_log.tuple_ = Tuple(undo_values, &subschema);
  }
  return undo_log;
}
auto MergeUndolog(const UndoLog &old_undo_log, const UndoLog &new_undo_log, const Schema *schema) -> UndoLog {
  if (old_undo_log.is_deleted_) {
    return old_undo_log;
  }

  std::vector<Value> values;
  auto sz = schema->GetColumnCount();
  std::vector<bool> modify_fields(sz, false);
  for (uint32_t i = 0, j = 0, k = 0; i < sz; i++) {
    bool m1 = old_undo_log.modified_fields_[i];
    bool m2 = new_undo_log.modified_fields_[i];
    if (m1 ^ m2) {
      modify_fields[i] = true;
      values.emplace_back(m1 ? old_undo_log.tuple_.GetValue(schema, j++) : new_undo_log.tuple_.GetValue(schema, k++));
    } else if (m1 && m2) {
      modify_fields[i] = true;
      values.emplace_back(old_undo_log.tuple_.GetValue(schema, j++));
      k++;
    }
  }
  Schema subschema = GetSubSchema(modify_fields, schema);
  return UndoLog{false, std::move(modify_fields), Tuple{values, &subschema}, old_undo_log.ts_,
                 old_undo_log.prev_version_};
}
void DeleteTuple(TableInfo *table_info, TransactionManager *txn_mgr, Transaction *txn, const Tuple &old_tuple,
                 TupleMeta old_meta, RID *rid) {
  const TupleMeta meta{txn->GetTransactionTempTs(), true};
  if (old_meta.ts_ == txn->GetTransactionTempTs()) {
    auto version_link_op = txn_mgr->GetVersionLink(*rid);
    if (version_link_op.has_value() && version_link_op->prev_.IsValid()) {
      UndoLog undo_log = GenerateDiffLog(old_tuple, old_meta, Tuple{}, TupleMeta{txn->GetTransactionTempTs(), true},
                                         &table_info->schema_);
      UndoLog old_undo_log = txn->GetUndoLog(version_link_op->prev_.prev_log_idx_);
      txn->ModifyUndoLog(version_link_op->prev_.prev_log_idx_,
                         MergeUndolog(old_undo_log, undo_log, &table_info->schema_));
    }
  } else {
    LockAndCheck(txn_mgr, *rid, txn, table_info);
    auto version_link_op = txn_mgr->GetVersionLink(*rid);
    UndoLog undo_log = GenerateDiffLog(old_tuple, old_meta, Tuple{}, TupleMeta{txn->GetTransactionTempTs(), true},
                                       &table_info->schema_);
    if (version_link_op.has_value()) {
      undo_log.prev_version_ = version_link_op->prev_;
    }
    VersionUndoLink new_version_link;
    new_version_link.prev_ = txn->AppendUndoLog(undo_log);
    new_version_link.in_progress_ = true;
    txn_mgr->UpdateVersionLink(*rid, new_version_link);
    txn->AppendWriteSet(table_info->oid_, *rid);
  }
  table_info->table_->UpdateTupleMeta(meta, *rid);
}
void InsertTuple(const IndexInfo *primary_index, const TableInfo *table_info, TransactionManager *txn_mgr,
                 Transaction *txn, Tuple *tuple) {
  const TupleMeta meta{txn->GetTransactionTempTs(), false};
  std::vector<RID> res;
  const auto key =
      tuple->KeyFromTuple(table_info->schema_, primary_index->key_schema_, primary_index->index_->GetKeyAttrs());
  primary_index->index_->ScanKey(key, &res, txn);
  if (res.empty()) {
    auto rid_opt = table_info->table_->InsertTuple(meta, *tuple);
    LockVersionLick(txn_mgr, *rid_opt);

    bool is_inserted = primary_index->index_->InsertEntry(key, rid_opt.value(), txn);
    txn->AppendWriteSet(table_info->oid_, *rid_opt);
    if (is_inserted) {
      txn_mgr->UpdateVersionLink(*rid_opt, std::nullopt);
    } else {
      SetTxnTainted(txn);
      // std::cerr<<"Thread "<<std::this_thread::get_id()<<":
      // Inserted"<<key.ToString(&primary_index->key_schema_)<<"failed because primary key already exists\n";
    }
  } else {
    RID rid = res[0];
    const auto &[origin_meta, origin_tuple] = table_info->table_->GetTuple(rid);

    if (!origin_meta.is_deleted_) {
      SetTxnTainted(txn);
    } else {
      if (meta.ts_ != origin_meta.ts_) {
        LockAndCheck(txn_mgr, rid, txn, table_info);
        UndoLog undolog = GenerateDiffLog(origin_tuple, origin_meta, *tuple, meta, &table_info->schema_);
        undolog.prev_version_ = txn_mgr->GetVersionLink(rid)->prev_;
        txn_mgr->UpdateVersionLink(rid, VersionUndoLink{txn->AppendUndoLog(undolog), true});
        txn->AppendWriteSet(table_info->oid_, rid);
      }
      table_info->table_->UpdateTupleInPlace(meta, *tuple, rid);
    }
  }
  // std::cerr << "Thread " << std::this_thread::get_id() << ' ' << (key.ToString(&primary_index->key_schema_))
  //           << std::endl;
}
}  // namespace bustub
