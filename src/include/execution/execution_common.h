#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);
void SetTxnTainted(Transaction *txn);
auto IsWriteWriteConflict(RID *rid, const TableInfo *table_info, Transaction *txn) -> bool;
auto LockVersionLick(TransactionManager *txn_mgr, RID *rid) -> bool;
void LockAndCheck(TransactionManager *txn_mgr, RID rid, Transaction *txn, const TableInfo *table_info);
auto GetSubSchema(const UndoLog &undo_log, const Schema *schema) -> Schema;
auto GetSubSchema(const std::vector<bool> &modified_fields_, const Schema *schema) -> Schema;
auto GenerateDiffLog(const Tuple &old_tuple, const TupleMeta &old_meta, const Tuple &new_tuple,
                     const TupleMeta &new_meta, const Schema *schema) -> UndoLog;
auto MergeUndolog(const UndoLog &old_undo_log, const UndoLog &new_undo_log, const Schema *schema) -> UndoLog;
void InsertTuple(const IndexInfo *primary_index, const TableInfo *table_info, TransactionManager *txn_mgr,
                 Transaction *txn, Tuple *tuple);
void DeleteTuple(TableInfo *table_info, TransactionManager *txn_mgr, Transaction *txn, const Tuple &old_tuple,
                 TupleMeta old_meta, RID *rid);
// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
