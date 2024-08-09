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
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"
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

  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  while (child_executor_->Next(tuple, rid)) {
    DeleteTuple(table_info, txn_mgr, txn, *tuple, table_info->table_->GetTupleMeta(*rid), rid);
    delete_cnt++;
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, delete_cnt);
  *tuple = Tuple(values, &GetOutputSchema());
  is_done_ = true;
  return true;
}

}  // namespace bustub
