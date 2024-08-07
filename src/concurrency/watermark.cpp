#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  current_reads_[read_ts]++;
  times_.insert(read_ts);
  watermark_ = *times_.begin();
  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
    times_.erase(read_ts);
    if (!times_.empty()) {
      watermark_ = *times_.begin();
    }
  }
}

}  // namespace bustub
