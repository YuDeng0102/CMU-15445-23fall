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
#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include <sys/types.h>
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

  while (child_executor_->Next(tuple, rid)) {
    auto t_rid = table_info->table_->InsertTuple(TupleMeta({0, false}), *tuple);
    *rid = *t_rid;
    for (auto index : indexs) {
      auto key_tuple = tuple->KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, *rid, this->exec_ctx_->GetTransaction());
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
