//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <optional>
#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include <sys/types.h>
#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }

  int insert_cnt = 0;
  auto catalog = this->exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto indexs = catalog->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  while (child_executor_->Next(tuple, rid)) {
    for (auto index : indexs) {
      const auto key_tuple = tuple->KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      std::vector<RID>res;
      index->index_->ScanKey(key_tuple,&res, txn);
      if(!res.empty()&&index->is_primary_key_) {
        txn->SetTainted();
        throw ExecutionException("index conflict");
      }
      // index->index_->InsertEntry(key_tuple, *rid, this->exec_ctx_->GetTransaction());
    }
    auto t_rid = table_info->table_->InsertTuple(TupleMeta({txn->GetTransactionTempTs(), false}), *tuple);
    *rid = *t_rid;
    txn->AppendWriteSet(table_info->oid_, *rid);

    txn_mgr->UpdateVersionLink(*rid, std::nullopt);
    
    for (auto index : indexs) {
      const auto key_tuple = tuple->KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      std::vector<RID>res;
     
      if(!index->index_->InsertEntry(key_tuple, *rid, txn)) {
        txn->SetTainted();
        throw ExecutionException("index conflict!");
      }
     
    }
    insert_cnt++;
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, insert_cnt);
  *tuple = Tuple(values, &GetOutputSchema());
  is_done_ = true;
  return true;
}

}  // namespace bustub
