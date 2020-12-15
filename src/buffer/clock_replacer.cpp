//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  in_CR_ = std::vector<bool> (num_pages, false);
  ref_flag_ = std::vector<bool> (num_pages, false);
  size_ = 0;
  max_size_ = num_pages;
  clock_hand_ = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (Size() == 0)
    return false;
  latch_.lock();
  while (!(in_CR_[clock_hand_] && (!ref_flag_[clock_hand_]))) {
    if (ref_flag_[clock_hand_])
      ref_flag_[clock_hand_] = false;
    clock_hand_ = (clock_hand_ + 1) % max_size_;
  }
  *frame_id = clock_hand_;
  in_CR_[clock_hand_] = false;
  --size_;
  latch_.unlock();
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_size_) {
    LOG_INFO("ClockReplacer::Pin:Invalid frame id!");
  } else {
    latch_.lock();
    if (in_CR_[frame_id]) {
      in_CR_[frame_id] = false;
      ref_flag_[frame_id] = false;
      --size_;
    }
    latch_.unlock();
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_size_) {
    LOG_INFO("ClockReplacer::Unpin:Invalid frame id!");
  } else {
    latch_.lock();
    if (!in_CR_[frame_id]) {
      in_CR_[frame_id] = true;
      ++size_;
    }
    ref_flag_[frame_id] = true;
    latch_.unlock();
  }
}

size_t ClockReplacer::Size() {
  latch_.lock();
  size_t res = size_;
  latch_.unlock();
  return res;
}

}  // namespace bustub
