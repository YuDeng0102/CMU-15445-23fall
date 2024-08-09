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
#include "execution/execution_common.h"
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
    if (indexs.empty()) {
      auto rid_op = table_info->table_->InsertTuple({txn->GetTransactionTempTs(), false}, *tuple);
      txn_mgr->UpdateVersionLink(*rid_op, std::nullopt);
      txn->AppendWriteSet(table_info->oid_, rid_op.value());
    } else {
      InsertTuple(indexs[0], table_info, txn_mgr, txn, tuple);
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
