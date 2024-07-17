//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <mutex>

#include "binder/table_ref/bound_base_table_ref.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t f_id;
  if (!free_list_.empty()) {
    f_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&f_id)) {
      return nullptr;
    }
  }
  auto page = pages_ + f_id;
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto feature = promise.get_future();
    disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise)});
    feature.get();
    page->is_dirty_ = false;
  }
  *page_id = AllocatePage();
  if (page->page_id_ != INVALID_PAGE_ID) {
    page_table_.erase(page->page_id_);
  }
  page_table_.insert({*page_id, f_id});
  page->page_id_ = *page_id;
  page->ResetMemory();
  page->pin_count_ = 1;
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  Page *page;
  if (page_table_.count(page_id) != 0U) {
    frame_id = page_table_.at(page_id);
    page = pages_ + frame_id;
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  page = pages_ + frame_id;
  if (page->IsDirty()) {
    auto promise1 = disk_scheduler_->CreatePromise();
    auto feature1 = promise1.get_future();
    disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise1)});
    feature1.get();
    page->is_dirty_ = false;
  }
  page_table_.erase(page->page_id_);
  page_table_.insert({page_id, frame_id});
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  auto promise2 = disk_scheduler_->CreatePromise();
  auto feature2 = promise2.get_future();
  disk_scheduler_->Schedule({false, page->data_, page->page_id_, std::move(promise2)});
  feature2.get();

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return false;
  }
  auto frame_id = page_table_.at(page_id);
  auto page = pages_ + frame_id;
  if (page->pin_count_ == 0) {
    return false;
  }
  page->pin_count_--;
  page->is_dirty_ |= is_dirty;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return false;
  }
  auto frame_id = page_table_.at(page_id);
  auto page = pages_ + frame_id;

  auto promise = disk_scheduler_->CreatePromise();
  auto feature = promise.get_future();
  disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise)});
  feature.get();
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto [k, w] : page_table_) {
    FlushPage(k);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return true;
  }
  auto frame_id = page_table_.at(page_id);
  auto page = pages_ + frame_id;
  if (page->pin_count_ != 0U) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
