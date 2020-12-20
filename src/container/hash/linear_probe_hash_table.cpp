//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  header_page_id_ = INVALID_PAGE_ID;
  header_page_ = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
  header_page_->SetSize(num_buckets);
  header_page_->SetPageId(header_page_id_);
  page_id_t block_page_id;
  for (size_t i = 0; i < num_buckets; ++i) {
    buffer_pool_manager_->NewPage(&block_page_id);
    header_page_->AddBlockPageId(block_page_id);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  bool found = false;
  auto hash_ind = hash_fn_.GetHash(key) % GetSize();
  auto blk_pg_id = header_page_->GetBlockPageId(hash_ind);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(blk_pg_id));
  for (size_t i = 0; i < (BLOCK_ARRAY_SIZE - 1) / 8 + 1; ++i) {
    if (!block_page->IsOccupied(i)) {
      break;
    } else if (block_page->IsReadable(i) && comparator_(block_page->KeyAt(i), key) == 0) {
      result->push_back(block_page->ValueAt(i));
      found = true;
    }
  }
  buffer_pool_manager_->UnpinPage(blk_pg_id, true);
  return found;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool res = false;
  auto hash_ind = hash_fn_.GetHash(key) % GetSize();
  auto blk_pg_id = header_page_->GetBlockPageId(hash_ind);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(blk_pg_id));
  for (size_t i = 0; i < (BLOCK_ARRAY_SIZE - 1) / 8 + 1; ++i) {
    if (!block_page->IsOccupied(i))
      break;
    if (block_page->IsReadable(i) && comparator_(block_page->KeyAt(i),key) == 0 && block_page->ValueAt(i) == value) {
      buffer_pool_manager_->UnpinPage(blk_pg_id, true);
      return false;
    }
  }
  for (size_t i = 0; i < (BLOCK_ARRAY_SIZE - 1) / 8 + 1; ++i) {
    if (!block_page->IsReadable(i)) {
      block_page->Insert(i, key, value);
      res = true;
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(blk_pg_id, true);
  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool res = false;
  auto hash_ind = hash_fn_.GetHash(key) % GetSize();
  auto blk_pg_id = header_page_->GetBlockPageId(hash_ind);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(blk_pg_id));
  for (size_t i = 0; i < (BLOCK_ARRAY_SIZE - 1) / 8 + 1; ++i) {
    if (!block_page->IsOccupied(i)) {
      break;
    }
    if (block_page->IsReadable(i) && comparator_(block_page->KeyAt(i), key) == 0 && block_page->ValueAt(i) == value) {
      block_page->Remove(i);
      res = true;
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(blk_pg_id, true);
  return res;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return header_page_->GetSize();
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
