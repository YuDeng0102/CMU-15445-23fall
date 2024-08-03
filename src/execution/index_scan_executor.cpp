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

  std::vector<RID> result;
  auto key = Tuple(std::vector<Value>({plan_->pred_key_->val_}), &index_info->key_schema_);
  htable->ScanKey(key, &result, this->exec_ctx_->GetTransaction());
  if (result.empty()) {
    return false;
  }
  *rid = result[0];
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto [mata, tp] = table_info->table_->GetTuple(*rid);
  if (mata.is_deleted_) {
    return false;
  }
  *tuple = tp;
  return true;
}

}  // namespace bustub
