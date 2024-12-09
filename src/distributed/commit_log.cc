#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  return log_entry_num_;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  log_entry_num_++;
 
  // Write the log entry to the log blocks
  // The log entry format is:
  // | txn_id | block_id | block_state |
  for (auto &op : ops) {
    std::vector<u8> buffer(sizeof(txn_id_t) + sizeof(block_id_t) + DiskBlockSize);
    auto txn_id_ptr = reinterpret_cast<txn_id_t *>(buffer.data());
    auto block_id_ptr = reinterpret_cast<block_id_t *>(buffer.data() + sizeof(txn_id_t));
    auto block_state_ptr = buffer.data() + sizeof(txn_id_t) + sizeof(block_id_t);
    txn_id_ptr[0] = txn_id;
    block_id_ptr[0] = op->block_id_;
    std::copy(op->new_block_state_.begin(), op->new_block_state_.end(), block_state_ptr);
    bm_->write_block_w_offset(current_log_offset_, buffer.data(), buffer.size());
    current_log_offset_ += buffer.size();
  }

}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  auto buffer = std::vector<u8>(sizeof(txn_id_t) + sizeof(block_id_t) + DiskBlockSize);
  auto txn_id_ptr = reinterpret_cast<txn_id_t *>(buffer.data());
  auto block_id_ptr = reinterpret_cast<block_id_t *>(buffer.data() + sizeof(txn_id_t));
  auto block_state_ptr = buffer.data() + sizeof(txn_id_t) + sizeof(block_id_t);
  txn_id_ptr[0] = txn_id;
  block_id_ptr[0] = 0;
  std::fill(block_state_ptr, block_state_ptr + DiskBlockSize, 0);
  bm_->write_block_w_offset(current_log_offset_, buffer.data(), buffer.size());
  current_log_offset_ += buffer.size();

  // Check if the log size exceeds the maximum size
  if (is_checkpoint_enabled_) {
    if (log_entry_num_ >= kMaxLogSize) {
      checkpoint();
    }
  }
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  bm_->zero_log_blocks();
  log_entry_num_ = 0;
  current_log_offset_ = 0;
  bm_->flush();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  auto buffer = std::vector<u8>(sizeof(txn_id_t) + sizeof(block_id_t) + DiskBlockSize);
  auto txn_id_ptr = reinterpret_cast<txn_id_t *>(buffer.data());
  auto block_id_ptr = reinterpret_cast<block_id_t *>(buffer.data() + sizeof(txn_id_t));
  auto block_state_ptr = buffer.data() + sizeof(txn_id_t) + sizeof(block_id_t);

  auto data_buffer = std::vector<u8>(log_entry_num_ * buffer.size());
  usize redo_offset = 0;
  txn_id_t current_txn_id = 0;
  // Read the log blocks
  bm_->read_log_blocks(data_buffer.data(), data_buffer.size());

  // Redo the log entries
  for (usize i = 0; i < log_entry_num_; i++) {
    std::memcpy(buffer.data(), data_buffer.data() + i * buffer.size(), buffer.size());
    txn_id_t txn_id = txn_id_ptr[0];
    if (current_txn_id == 0) {
      current_txn_id = txn_id;
    }
    else if (current_txn_id != txn_id) {
      current_txn_id = 0;
      continue;
    }
    block_id_t block_id = block_id_ptr[0];
    if (block_id == 0) {
      for (usize j = redo_offset; j < i; j++) {
        std::memcpy(buffer.data(), data_buffer.data() + j * buffer.size(), buffer.size());
        txn_id = txn_id_ptr[0];
        block_id = block_id_ptr[0];
        std::vector<u8> block_state(block_state_ptr, block_state_ptr + DiskBlockSize);
        bm_->write_block(block_id, block_state.data());
      }
      redo_offset = i + 1;
      current_txn_id = 0;
    }
  }
}
}; // namespace chfs