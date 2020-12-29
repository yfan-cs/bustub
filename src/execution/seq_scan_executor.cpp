//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  const auto &catalog = GetExecutorContext()->GetCatalog();
  table_ = catalog->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = std::make_unique<TableIterator>(table_->Begin(GetExecutorContext()->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto &iter = *iter_;
  while (iter != table_->End()) {
    auto tup = *(iter++);
    auto eval = true;
    if (plan_->GetPredicate() != nullptr) {
      eval = plan_->GetPredicate()->Evaluate(&tup, GetOutputSchema()).GetAs<bool>();
    }
    if (eval) {
      *tuple = Tuple(tup);
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
