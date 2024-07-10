//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <mutex>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  *frame_id = -1;
  for (auto &[f_id, node] : node_store_) {
    if (*frame_id == -1) {
      if (node.IsEvictable()) {
        *frame_id = f_id;
      }
    } else {
      if (!node_store_.at(f_id).IsEvictable()) {
        continue;
      }
      if (node < node_store_.at(*frame_id)) {
        *frame_id = f_id;
      }
    }
  }
  if (*frame_id == -1) {
    return false;
  }
  curr_size_--;
  node_store_.at(*frame_id).ClearHistory();
  node_store_.at(*frame_id).SetEvictable(false);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("invalid frame_id!");
  }

  if (!static_cast<bool>(node_store_.count(frame_id))) {
    if (node_store_.size() == replacer_size_) {
      return;
    }
    node_store_.insert({frame_id, LRUKNode(current_timestamp_, k_)});
  } else {
    auto &node = node_store_.at(frame_id);
    node.InsertRecord(current_timestamp_);
  }
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_ || !static_cast<bool>(node_store_.count(frame_id))) {
    throw Exception("invalid frame_id!");
  }
  auto &node = node_store_.at(frame_id);
  if (!node.IsEvictable() && set_evictable) {
    ++curr_size_;
  } else if (node.IsEvictable() && !set_evictable) {
    --curr_size_;
  }
  node_store_.at(frame_id).SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (!static_cast<bool>(node_store_.count(frame_id))) {
    return;
  }
  auto &node = node_store_.at(frame_id);
  if (!node.IsEvictable()) {
    throw Exception("Can't remove");
  }
  node_store_.at(frame_id).ClearHistory();
  node_store_.at(frame_id).SetEvictable(false);
  --curr_size_;
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
