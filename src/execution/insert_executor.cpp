//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), index_(0) {}

void InsertExecutor::Init() {
  const auto &catalog = GetExecutorContext()->GetCatalog();
  table_ = catalog->GetTable(plan_->TableOid())->table_.get();
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple t;
  RID r;
  if (plan_->IsRawInsert()) {
    auto numTups = plan_->RawValues().size();
    if (index_ >= numTups) return false;
    const auto &catalog = GetExecutorContext()->GetCatalog();
    const Schema *schema = &catalog->GetTable(plan_->TableOid())->schema_;
    t = Tuple(plan_->RawValuesAt(index_++), schema);
    auto success = table_->InsertTuple(t, &r, GetExecutorContext()->GetTransaction());
    return success;
  }
  if (child_executor_->Next(&t, &r)) {
    auto success = table_->InsertTuple(t, &r, GetExecutorContext()->GetTransaction());
    return success;
  }
  return false;
}

}  // namespace bustub
