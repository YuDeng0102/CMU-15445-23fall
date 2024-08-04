#include "execution/execution_common.h"
#include <cstdint>
#include <optional>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
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

auto GetSubSchema(const UndoLog& undo_log,const Schema* schema)->Schema {
   std::vector<Column>colums;
    for(uint32_t i=0;i<undo_log.modified_fields_.size();i++){
      if(undo_log.modified_fields_[i]){
          auto& tem_col=schema->GetColumn(i);
          colums.emplace_back(tem_col.GetName(),tem_col);
      }
    }
    return Schema(colums);
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if(base_meta.is_deleted_&&undo_logs.empty()) {
      return std::nullopt;
  }
  std::vector<Value> values;
  for(uint32_t i=0;i<schema->GetColumnCount();i++){
       values.emplace_back(base_tuple.GetValue(schema,i));
  }
  for(auto cnt=0U;cnt<undo_logs.size();cnt++){
    auto& undo_log=undo_logs[cnt];
    if(undo_log.is_deleted_&&cnt==undo_logs.size()-1){
      return std::nullopt;
    }
    std::vector<Column>colums;
    std::vector<uint32_t>colum_idx;
    for(uint32_t i=0;i<undo_log.modified_fields_.size();i++){
      if(undo_log.modified_fields_[i]){
          auto& tem_col=schema->GetColumn(i);
          colums.emplace_back(tem_col.GetName(),tem_col);
          colum_idx.emplace_back(i);
      }
    }
    Schema subschema(colums);
    for(uint32_t i=0;i<colums.size();i++){
      values[colum_idx[i]]=undo_log.tuple_.GetValue(&subschema,i);
    }
  }
  return std::make_optional(Tuple(values,schema));
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  
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

  auto iter=table_heap->MakeIterator();
  while(!iter.IsEnd()) {
    const auto& [meta,tuple]=iter.GetTuple();
    auto rid=iter.GetRID();
    ++iter;
    const Schema& schema=table_info->schema_;
    std::string tuple_context;
    for(uint32_t i=0;i<schema.GetColumnCount();i++) {
      const auto&val=tuple.GetValue(&table_info->schema_,i);
      if(val.IsNull()) {
        tuple_context.append("<NULL>");
      }else{
        tuple_context.append(val.ToString());
      }
      if(i!=table_info->schema_.GetColumnCount()-1) {
        tuple_context.append(", ");
      }
    }
    
    auto ts_string=meta.ts_>=TXN_START_ID?"txn"+fmt::to_string(meta.ts_-TXN_START_ID):fmt::to_string(meta.ts_);
    
    fmt::println(stderr,"RID={}/{} ts={}{} tuple=({})",rid.GetPageId(),rid.GetSlotNum(),ts_string,meta.is_deleted_?" <delete marker> ":"",tuple_context);
    auto undo_link=txn_mgr->GetUndoLink(rid);
    if(undo_link.has_value()) {
      while(undo_link->IsValid()){
        std::unique_lock<std::shared_mutex> l(txn_mgr->txn_map_mutex_);
        auto tem_txn=txn_mgr->txn_map_[undo_link->prev_txn_];
        const auto& undo_log=tem_txn->GetUndoLog(undo_link->prev_log_idx_);
        l.unlock();

        if(undo_log.is_deleted_) {
           fmt::println(stderr,"\ttxn{} <del> ts={}",tem_txn->GetTransactionId()-TXN_START_ID,tem_txn->GetReadTs());
        }
        else {
          std::string txn_context;
          Schema subschema=GetSubSchema(undo_log,&schema);
          for(uint32_t i=0;i<schema.GetColumnCount();i++) {
            if(undo_log.modified_fields_[i]) {
              txn_context+=undo_log.tuple_.GetValue(&subschema,i).ToString();
            }else {
              txn_context+='_';
            }
             if(i!=schema.GetColumnCount()-1) {
               txn_context.append(", ");
              }
          }
          fmt::println(stderr,"\ttxn{} ({}) ts={}",tem_txn->GetTransactionId()-TXN_START_ID,txn_context,tem_txn->GetReadTs());
        }
        undo_link=undo_log.prev_version_;
      }
    }
  }
}

}  // namespace bustub
