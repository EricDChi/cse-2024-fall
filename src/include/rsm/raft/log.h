#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include "filesystem/operations.h"
#include "rsm/raft/protocol.h"
#include "common/util.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    RaftLog(const std::string &data_path);
    ~RaftLog();

    /* Lab3: Your code here */
    auto persist_metadata(int current_term, int voted_for, int commit_index, int last_applied) -> bool;
    auto persist_log(std::vector<LogEntry<Command>> &log_entries) -> bool;
    auto persist_snapshot(int last_included_index, int last_included_term, std::vector<u8> &snapshot_data) -> bool;
    auto recover_metadata(int &current_term, int &voted_for, int &commit_index, int &last_applied) -> bool;
    auto recover_log(std::vector<LogEntry<Command>> &log_entries) -> bool;
    auto recover_snapshot(int &last_included_index, int &last_included_term, std::vector<u8> &snapshot_data) -> bool;

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    std::shared_ptr<FileOperation> operation_;
    inode_id_t log_id;
    inode_id_t metadata_id;
    inode_id_t snapshot_id;
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    /* Lab3: Your code here */
    this->bm_ = bm;
}

template <typename Command>
RaftLog<Command>::RaftLog(const std::string &data_path)
{
    bool is_initialized = is_file_exist(data_path);

    // However, as we assume that total size of raft log is always smaller than 64K
    // and size of each block is 4K, we can use less blocks to store the log
    auto block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
    bm_ = block_manager;

    // initialize the file operation
    if (is_initialized) {
        auto origin_res = FileOperation::create_from_raw(block_manager);
        if (origin_res.is_err()) {
            std::cerr << "Original FS is bad, please remove files manually." << std::endl;
            exit(1);
        }

        operation_ = origin_res.unwrap();

        metadata_id = operation_->lookup(1, "raft_metadata").unwrap();
        log_id = operation_->lookup(1, "raft_log").unwrap();
        snapshot_id = operation_->lookup(1, "raft_snapshot").unwrap();
    } else {
        // no need to make many inodes
        operation_ = std::make_shared<FileOperation>(block_manager, 4);

        // make root directory
        auto init_res = operation_->alloc_inode(InodeType::Directory);
        if (init_res.is_err()) {
            std::cerr << "Cannot allocate inode for root directory." << std::endl;
            exit(1);
        }
        auto root_id = init_res.unwrap();

        // create a file for the metadata
        auto make_metadata_res = operation_->mkfile(root_id, "raft_metadata");
        if (make_metadata_res.is_err()) {
            std::cerr << "Cannot create file for the metadata." << std::endl;
            exit(1);
        }
        metadata_id = make_metadata_res.unwrap();

        // create a file for the log
        auto make_log_res = operation_->mkfile(root_id, "raft_log");
        if (make_log_res.is_err()) {
            std::cerr << "Cannot create file for the log." << std::endl;
            exit(1);
        }
        log_id = make_log_res.unwrap();

        // create a file for the snapshot
        auto make_snapshot_res = operation_->mkfile(root_id, "raft_snapshot");
        if (make_snapshot_res.is_err()) {
            std::cerr << "Cannot create file for the snapshot." << std::endl;
            exit(1);
        }
        snapshot_id = make_snapshot_res.unwrap();
    }
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
    this->bm_->flush();
}

/* Lab3: Your code here */
template <typename Command>
auto RaftLog<Command>::persist_metadata(int current_term, int voted_for, int commit_index, int last_applied) -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<int> metadata(4);
    metadata[0] = current_term;
    metadata[1] = voted_for;
    metadata[2] = commit_index;
    metadata[3] = last_applied;

    auto metadata_data = reinterpret_cast<u8 *>(metadata.data());
    std::vector<u8> data(metadata_data, metadata_data + 4 * sizeof(int));
    auto res = operation_->write_file(metadata_id, data);
    if (res.is_err()) {
        return false;
    }

    return true;
}

template <typename Command>
auto RaftLog<Command>::persist_log(std::vector<LogEntry<Command>> &log_entries) -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<u8> data;
    for (auto &log_entry : log_entries) {
        auto log_entry_data = reinterpret_cast<u8 *>(&log_entry);
        data.insert(data.end(), log_entry_data, log_entry_data + sizeof(LogEntry<Command>));
    }

    auto res = operation_->write_file(log_id, data);
    if (res.is_err()) {
        return false;
    }

    return true;
}

template <typename Command>
auto RaftLog<Command>::persist_snapshot(int last_included_index, int last_included_term, std::vector<u8> &snapshot_data) -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<u8> data;
    data.insert(data.end(), reinterpret_cast<u8 *>(&last_included_index), reinterpret_cast<u8 *>(&last_included_index) + sizeof(int));
    data.insert(data.end(), reinterpret_cast<u8 *>(&last_included_term), reinterpret_cast<u8 *>(&last_included_term) + sizeof(int));
    data.insert(data.end(), snapshot_data.begin(), snapshot_data.end());

    auto res = operation_->write_file(snapshot_id, data);
    if (res.is_err()) {
        return false;
    }

    return true;
}

template <typename Command>
auto RaftLog<Command>::recover_metadata(int &current_term, int &voted_for, int &commit_index, int &last_applied) -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<u8> data;
    auto res = operation_->read_file(metadata_id);
    if (res.is_err()) {
        return false;
    }
    data = res.unwrap();
    
    current_term = *reinterpret_cast<int *>(data.data());
    voted_for = *reinterpret_cast<int *>(data.data() + sizeof(int));
    commit_index = *reinterpret_cast<int *>(data.data() + 2 * sizeof(int));
    last_applied = *reinterpret_cast<int *>(data.data() + 3 * sizeof(int));

    return true;
}

template <typename Command>
auto RaftLog<Command>::recover_log(std::vector<LogEntry<Command>> &log_entries) -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<u8> data;
    auto res = operation_->read_file(log_id);
    if (res.is_err()) {
        return false;
    }
    data = res.unwrap();
    log_entries.clear();

    auto log_entries_data = reinterpret_cast<LogEntry<Command> *>(data.data());
    for (int i = 0; i < data.size() / sizeof(LogEntry<Command>); i++) {
        log_entries.push_back(log_entries_data[i]);
    }

    return true;
}

template <typename Command>
auto RaftLog<Command>::recover_snapshot(int &last_included_index, int &last_included_term, std::vector<u8> &snapshot_data) -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<u8> data;
    auto res = operation_->read_file(snapshot_id);
    if (res.is_err()) {
        return false;
    }
    data = res.unwrap();

    last_included_index = *reinterpret_cast<int *>(data.data());
    last_included_term = *reinterpret_cast<int *>(data.data() + sizeof(int));
    snapshot_data = std::vector<u8>(data.begin() + 2 * sizeof(int), data.end());

    return true;
}

} /* namespace chfs */
