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
#include <utility>
#include "catalog/catalog.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {
  // iter_=std::move(this->GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
  while (!iter_.IsEnd()) {
    val_.push_back(iter_.GetTuple());
    ++iter_;
  }
  viter_ = val_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (viter_ == val_.end()) {
    return false;
  }
  auto filter_predicate = plan_->filter_predicate_;
  auto [meta, tp] = *viter_;

  auto check = [&](Tuple *tp) -> bool {
    if (filter_predicate == nullptr) {
      return true;
    }
    auto val = filter_predicate->Evaluate(tp, GetOutputSchema());
    return !val.IsNull() && val.GetAs<bool>();
  };
  while (meta.is_deleted_ || !check(&tp)) {
    ++viter_;
    if (viter_ == val_.end()) {
      return false;
    }
    auto k = *viter_;
    meta = k.first;
    tp = std::move(k.second);
    if (!meta.is_deleted_ && check(&tp)) {
      break;
    }
  }
  *tuple = std::move(tp);
  *rid = tuple->GetRid();
  ++viter_;
  return true;
}

}  // namespace bustub
