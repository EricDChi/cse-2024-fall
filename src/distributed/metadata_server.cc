#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
// use 2PL to implement concurrency control
// transaction id is auto increment
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  if (!is_log_enabled_) {
    operation_mutex.lock();
    auto res = operation_->mk_helper(parent, name.data(), static_cast<InodeType>(type));
    if (res.is_err()) {
      operation_mutex.unlock();
      return 0;
    }

    if (this->is_log_enabled_) {
      commit_log->commit_log(commit_log->get_log_entry_num() + 1);
    }

    operation_mutex.unlock();
    return res.unwrap();
  }

  // if log is enabled
  // first record all changes to blocks in log
  // then commit the log
  // finally write all changes to blocks
  operation_mutex.lock();
  txn_id_t txn_id = commit_log->get_log_entry_num() + 1;
  std::vector<std::shared_ptr<BlockOperation>> ops;
  // auto res = operation_->mk_helper(parent, name.data(), static_cast<InodeType>(type));
  auto res = operation_->mknode_atomic(parent, name.data(), static_cast<InodeType>(type),ops);
  if (res.is_err()) {
    operation_mutex.unlock();
    return 0;
  }

  commit_log->append_log(txn_id, ops);
  commit_log->commit_log(txn_id);
  for (auto &op : ops) {
    auto write_res = operation_->block_manager_->write_block(op->block_id_, op->new_block_state_.data());

    if (write_res.is_err()) {
      operation_mutex.unlock();
      return 0;
    }
  }

  operation_mutex.unlock();
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // operation_mutex.lock();
  // auto res = operation_->unlink(parent, name.c_str());
  // operation_mutex.unlock();
  // if (res.is_err()) {
  //   return false;
  // }

  // return true;

  if (!is_log_enabled_) {
    operation_mutex.lock();
    auto lookup_res = operation_->lookup(parent, name.c_str());
    if (lookup_res.is_err()) {
      operation_mutex.unlock();
      return false;
    }
    inode_id_t id = lookup_res.unwrap();

    auto type_res = operation_->gettype(id);
    if (type_res.is_err()) {
      operation_mutex.unlock();
      return false;
    }
    InodeType type = type_res.unwrap();

    // if type is directory, call unlink
    // like implementation before
    if (type == InodeType::Directory) {
      auto unlink_res = operation_->unlink(parent, name.data());
      if (unlink_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }
      operation_mutex.unlock();
      return true;
    }
    // if type is file, something diffrent
    else if (type == InodeType::FILE) {
      std::vector<BlockInfo> block_infos = get_block_map(id);
      auto block_res = operation_->inode_manager_->get(id);
      if (block_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }
      block_id_t block_id = block_res.unwrap();

      // free inode and deallocate its block
      auto free_res = operation_->inode_manager_->free_inode(id);
      if (free_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }

      auto dealloc_res = operation_->block_allocator_->deallocate(block_id);
      if (dealloc_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }

      // free all blocks in block_infos
      for (auto &block_info : block_infos) {
        block_id_t block_id = std::get<0>(block_info);
        mac_id_t machine_id = std::get<1>(block_info);
        auto machine = clients_[machine_id];
        auto free_res = machine->call("free_block", block_id);
        if (free_res.is_err() || !free_res.unwrap()->as<bool>()) {
          operation_mutex.unlock();
          return false;
        }
      }

      // remove the file from directory
      std::list<DirectoryEntry> list;
      read_directory(operation_.get(), parent, list);
      std::string src = dir_list_to_string(list);
      src = rm_from_directory(src, name);
      std::vector<u8> content(src.begin(), src.end());
      auto write_res = operation_->write_file(parent, content);
      if (write_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }

      operation_mutex.unlock();
      return true;
    }
    operation_mutex.unlock();
    return true;
  }
  
  operation_mutex.lock();
  txn_id_t txn_id = commit_log->get_log_entry_num() + 1;
  std::vector<std::shared_ptr<BlockOperation>> ops;
  auto lookup_res = operation_->lookup_atomic(parent, name.data(), ops); 
  if (lookup_res.is_err()) {
    operation_mutex.unlock();
    return false;
  }

  inode_id_t id = lookup_res.unwrap();
  auto type_res = operation_->gettype(id);
  if (type_res.is_err()) {
    operation_mutex.unlock();
    return false;
  }
  InodeType type = type_res.unwrap();

  if (type == InodeType::Directory) {
    auto unlink_res = operation_->unlink_atomic(parent, name.data(), ops);
    if (unlink_res.is_err()) {
      operation_mutex.unlock();
      return false;
    }

    commit_log->append_log(txn_id, ops);
    commit_log->commit_log(txn_id);

    for (auto &op : ops) {
      auto write_res = operation_->block_manager_->write_block(op->block_id_, op->new_block_state_.data());
      if (write_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }
    }

    operation_mutex.unlock();
    return true;
  }
  else if (type == InodeType::FILE) {
    std::vector<BlockInfo> block_infos = get_block_map_atomic(id, ops);
    for (auto &block_info : block_infos) {
      block_id_t block_id = std::get<0>(block_info);
      mac_id_t machine_id = std::get<1>(block_info);
      auto machine = clients_[machine_id];
      auto free_res = machine->call("free_block", block_id);
      if (free_res.is_err() || !free_res.unwrap()->as<bool>()) {
        operation_mutex.unlock();
        return false;
      }
    }

    std::list<DirectoryEntry> list;
    read_directory_atomic(operation_.get(), parent, list, ops);
    for (auto &entry : list) {
      if (entry.name != name.data()) {
        continue;
      }

      inode_id_t inode_id = entry.id;
      auto get_res = operation_->inode_manager_->get_atomic(inode_id, ops);
      if (get_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }
      block_id_t block_id = get_res.unwrap();

      auto dealloc_res = operation_->block_allocator_->deallocate_atomic(block_id, ops);
      if (dealloc_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }

      auto free_res = operation_->inode_manager_->free_inode_atomic(inode_id, ops);
      if (free_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }

      std::string src = dir_list_to_string(list);
      src = rm_from_directory(src, name);
      std::vector<u8> content(src.begin(), src.end());
      auto write_res = operation_->write_file_atomic(parent, content, ops);
      if (write_res.is_err()) {
        operation_mutex.unlock();
        return false;
      }

      commit_log->append_log(txn_id, ops);
      commit_log->commit_log(txn_id);

      for (auto &op : ops) {
        auto write_res = operation_->block_manager_->write_block(op->block_id_, op->new_block_state_.data());
        if (write_res.is_err()) {
          operation_mutex.unlock();
          return false;
        }
      }

      break;
    }

    operation_mutex.unlock();
    return true;
  }
  operation_mutex.unlock();
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  auto res = operation_->lookup(parent, name.c_str());

  if (res.is_err()) {
    return 0;
  }

  return res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  auto res = operation_->get_block_infos(id);

  if (res.is_err()) {
    return {};
  }

  return res.unwrap();
}

auto MetadataServer::get_block_map_atomic(inode_id_t id,
                                          std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> std::vector<BlockInfo> {
  auto res = operation_->get_block_infos_atomic(id, ops);

  if (res.is_err()) {
    return {};
  }

  return res.unwrap();
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  client_mutex.lock();
  mac_id_t machine_id = generator.rand(1, num_data_servers);
  auto client = clients_[machine_id];
 
  auto res = client->call("alloc_block"); 
  if (res.is_err()) {
    client_mutex.unlock();
    return {0, 0, 0};
  }

  auto pair = res.unwrap()->as<std::pair<block_id_t, version_t>>();
  BlockInfo info = {pair.first, machine_id, pair.second};

  operation_mutex.lock();
  // add block info to inode
  auto add_res = operation_->add_block_info(id, info);
  if (add_res.is_err()) {
    operation_mutex.unlock();
    client_mutex.unlock();
    return {0, 0, 0};
  }

  operation_mutex.unlock();
  client_mutex.unlock();
  auto map = get_block_map(id);
  return info;
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  client_mutex.lock();
  auto client = clients_[machine_id];

  auto res = client->call("free_block", block_id);
  if (res.is_err() || !res.unwrap()->as<bool>()) {
    client_mutex.unlock();
    return false;
  }

  operation_mutex.lock();
  // remove block info from inode
  auto remove_res = operation_->remove_block_info(id, block_id, machine_id);
  if (remove_res.is_err()) {
    operation_mutex.unlock();
    client_mutex.unlock();
    return false;
  } 

  operation_mutex.unlock();
  client_mutex.unlock();
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.

  std::list<DirectoryEntry> entries;
  auto res = read_directory(operation_.get(), node, entries);
  if (res.is_err()) {
    return {};
  }

  std::vector<std::pair<std::string, inode_id_t>> result;

  for (auto &entry : entries) {
    result.push_back({entry.name, entry.id});
  }

  return result;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  auto res = operation_->get_type_attr(id);
  if (res.is_err()) {
    return {0, 0, 0, 0, 0};
  }

  auto tuple = res.unwrap();

  u8 type = static_cast<u8>(std::get<0>(tuple));
  FileAttr attr = std::get<1>(tuple);

  return {attr.size, attr.atime, attr.mtime, attr.ctime, type};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs