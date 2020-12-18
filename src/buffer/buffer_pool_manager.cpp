//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  latch_.lock();
  frame_id_t frame_id;

  // page_id already in page_table (pinned, or unpinned but not flushed)
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    Page *page = &pages_[frame_id];
    ++page->pin_count_;
    latch_.unlock();
    return page;
  }

  bool ok = false;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    ok = true;
  } else if (replacer_->Victim(&frame_id)) {
    ok = true;
  }

  if (ok) {
    replacer_->Pin(frame_id);
    Page *page = &pages_[frame_id];
    page_id_t page_id_rm = page->GetPageId();
    if (page_id_rm != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(page_id_rm, page->GetData());
    }
    page_table_.erase(page_id_rm);  // if key not exists, nothing happens
    page_table_[page_id] = frame_id;
    page->ResetMemory();
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    disk_manager_->ReadPage(page_id, page->GetData());
    latch_.unlock();
    return page;
  }
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();

  // first, check if page_id in page_table_
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  // second, check the pin count for that page > 0
  Page *page = &pages_[page_table_[page_id]];
  if (page->GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }

  // finally, unpin the page
  --page->pin_count_;
  page->is_dirty_ = is_dirty;
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }

  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();

  // first, check if the page_id in page_table_
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty())
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  latch_.unlock();

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();
  frame_id_t frame_id;
  bool ok = false;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    ok = true;
  } else if (replacer_->Victim(&frame_id)) {
    ok = true;
  }
  if (ok) {
    *page_id = disk_manager_->AllocatePage();
    replacer_->Pin(frame_id);
    Page *page = &pages_[frame_id];

    // flush old page
    page_id_t page_id_rm = page->GetPageId();
    if (page_id_rm != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(page_id_rm, page->GetData());
    }
    page_table_.erase(page_id_rm);  // if key not exists, nothing happens

    page_table_[*page_id] = frame_id;
    page->ResetMemory();
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    latch_.unlock();
    return page;
  }
  latch_.unlock();
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();

  // first, check if the page_id in page_table_
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  latch_.unlock();

  disk_manager_->DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty())
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
    replacer_->Pin(i);
  }
  latch_.lock();
  page_table_.clear();
  free_list_.clear();
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  latch_.unlock();
}

}  // namespace bustub
