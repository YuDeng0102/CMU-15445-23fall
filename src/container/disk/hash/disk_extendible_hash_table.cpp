//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  auto guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto head_page = guard.AsMut<ExtendibleHTableHeaderPage>();
  head_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = guard.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = head_page->HashToDirectoryIndex(hash);
  if (directory_idx >= head_page->MaxSize()) {
    return false;
  }

  auto directory_page_id = head_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }

  guard.Drop();
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory = directory_guard.template As<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  result->push_back(V());
  bool success = bucket->Lookup(key, result->back(), cmp_);
  if (success) {
    return true;
  }
  result->pop_back();
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = guard.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = head_page->HashToDirectoryIndex(hash);
  if (directory_idx >= head_page->MaxSize()) {
    return false;
  }

  auto directory_page_id = head_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return InsertToNewDirectory(head_page, directory_idx, hash, key, value);
  }

  guard.Drop();
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket->IsFull()) {
    return bucket->Insert(key, value, cmp_);
  }
  // split bucket
  while (directory->Size() <= directory->MaxSize()) {
    while (directory->GetLocalDepth(bucket_idx) < directory->GetGlobalDepth()) {
      page_id_t new_page_id;
      auto write_guard = bpm_->NewPageGuarded(&new_page_id).UpgradeWrite();
      auto new_bucket = write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      new_bucket->Init(bucket_max_size_);
      auto local_depth = directory->GetLocalDepth(bucket_idx);
      auto highest_bit = 1 << local_depth;
      for (uint32_t i = 0; i < bucket->Size(); i++) {
        auto [k, v] = bucket->EntryAt(i);
        auto hash = Hash(k);
        if (hash & highest_bit) {
          new_bucket->Insert(k, v, cmp_);
          bucket->Remove(k, cmp_);
        }
      }

      // Update directory

      auto global_depth = directory->GetGlobalDepth();
      auto local_mask = directory->GetLocalDepthMask(bucket_idx);
      for (uint32_t i = 0; i < 1U << (global_depth - local_depth); i++) {
        auto idx = (bucket_idx & local_mask) + (i << local_depth);
        if ((i & 1) != 0U) {
          UpdateDirectoryMapping(directory, idx, new_page_id, local_depth + 1);
        } else {
          directory->IncrLocalDepth(idx);
        }
      }
      if (bucket_idx & highest_bit) {
        bucket = new_bucket;
      }
      if (!bucket->IsFull()) {
        return bucket->Insert(key, value, cmp_);
      }
    }
    if (directory->Size() == directory->MaxSize()) {
      break;
    }
    directory->IncrGlobalDepth();
    bucket_idx = directory->HashToBucketIndex(hash);
  }
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  auto directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
  return InsertToNewBucket(directory, directory->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_page_id;
  auto write_guard = bpm_->NewPageGuarded(&new_page_id).UpgradeWrite();
  auto new_bucket = write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, new_page_id);
  return new_bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth) {
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = guard.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = head_page->HashToDirectoryIndex(hash);
  if (directory_idx >= head_page->MaxSize()) {
    return false;
  }

  auto directory_page_id = head_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }

  guard.Drop();
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // PrintHT();
  if (!bucket->Remove(key, cmp_)) {
    return false;
  }

  if (bucket->IsEmpty()) {
    bucket_guard.Drop();
    auto merge_guard = bpm_->FetchPageRead(bucket_page_id);
    auto merge_bucket = merge_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
    MergeBucket(directory, bucket_idx & directory->GetLocalDepthMask(bucket_idx), merge_bucket);
    directory->Shrink();
  }
  return true;
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                    const ExtendibleHTableBucketPage<K, V, KC> *bucket) {
  auto depth = directory->GetLocalDepth(bucket_idx);
  if (depth == 0) {
    return;
  }
  auto split_image_idx = directory->GetSplitImageIndex(bucket_idx);
  if (directory->GetLocalDepth(split_image_idx) != depth) {
    return;
  }
  auto split_image_page_id = directory->GetBucketPageId(split_image_idx);
  // PrintHT();
  // LOG_DEBUG("split_id:%d\tbucket_id:%d", split_image_page_id, bucket_idx);
  // if (split_image_page_id == directory->GetBucketPageId(bucket_idx)) {

  // }
  assert(split_image_page_id != directory->GetBucketPageId(bucket_idx));

  auto split_guard = bpm_->FetchPageRead(split_image_page_id);
  auto split_bucket = split_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!split_bucket->IsEmpty() && !bucket->IsEmpty()) {
    return;
  }
  int old_local_depth = directory->GetLocalDepth(bucket_idx);
  int global_depth = directory->GetGlobalDepth();
  bool merge_to_new = bucket->IsEmpty();
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  for (int i = 0; i < 1 << (global_depth - old_local_depth); i++) {
    directory->SetBucketPageId((merge_to_new ? bucket_idx : split_image_idx) + (i << old_local_depth),
                               (merge_to_new ? split_image_page_id : bucket_page_id));
    directory->DecrLocalDepth(bucket_idx + (i << old_local_depth));
    directory->DecrLocalDepth(split_image_idx + (i << old_local_depth));
  }
  if (merge_to_new) {
    bucket = split_bucket;
  } else {
    split_guard.Drop();
  }
  MergeBucket(directory, bucket_idx & directory->GetLocalDepthMask(bucket_idx), merge_to_new ? split_bucket : bucket);
}
template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
