#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);
  const auto total_version_per_block = DiskBlockSize / sizeof(version_t);
  const auto version_block_cnt = (KDefaultBlockCnt - 1) / total_version_per_block + 1;

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_block_cnt, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, version_block_cnt, true));
    for (block_id_t i = 0; i < version_block_cnt; i++) {
      this->block_allocator_->bm->zero_block(i);
    }
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.

  if (offset + len > block_allocator_->bm->block_size()) {
    return std::vector<u8> {};
  }

  std::vector<u8> buffer(block_allocator_->bm->block_size());

  const auto total_version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
  auto version_block_id = block_id / total_version_per_block;
  auto version_block_index = block_id % total_version_per_block;

  auto res = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (res.is_err()) {
    return std::vector<u8> {};
  }

  version_t current_version = *reinterpret_cast<version_t *>(buffer.data() + version_block_index * sizeof(version_t));
  if (current_version != version) {
    return std::vector<u8> {};
  }

  res = block_allocator_->bm->read_block(block_id, buffer.data());
  if (res.is_err()) {
    return std::vector<u8> {};
  }
  std::vector<u8> result(buffer.begin() + offset, buffer.begin() + offset + len);

  return result;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.

  if (buffer.size() + offset > block_allocator_->bm->block_size()) {
    return false;
  }

  auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(),
                                                       offset, buffer.size());
  if (res.is_err()) {
    return false;
  }

  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.

  auto alloc_res = block_allocator_->allocate();
  if (alloc_res.is_err()) {
    return {0, 0};
  }

  block_id_t block_id = alloc_res.unwrap();
  version_t version = 0;

  const auto total_version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
  auto version_block_id = block_id / total_version_per_block;
  auto version_block_index = block_id % total_version_per_block;
  std::vector<u8> buffer(block_allocator_->bm->block_size());

  auto read_res = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (read_res.is_err()) {
    return {0, 0};
  }

  // Update the version of the block
  version = *reinterpret_cast<version_t *>(buffer.data() + version_block_index * sizeof(version_t));
  version += 1;
  read_res = block_allocator_->bm->write_partial_block(version_block_id, reinterpret_cast<u8 *>(&version),
                                                  version_block_index * sizeof(version_t), sizeof(version_t));
  if (read_res.is_err()) {
    return {0, 0};
  }

  return {block_id, version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.

  auto dealloc_res = block_allocator_->deallocate(block_id);
  if (dealloc_res.is_err()) {
    return false;
  }

  version_t version = 0;

  const auto total_version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
  auto version_block_id = block_id / total_version_per_block;
  auto version_block_index = block_id % total_version_per_block;
  std::vector<u8> buffer(block_allocator_->bm->block_size());

  auto read_res = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (read_res.is_err()) {
    return false;
  }

  // Update the version of the block
  version = *reinterpret_cast<version_t *>(buffer.data() + version_block_index * sizeof(version_t));
  version += 1;
  read_res = block_allocator_->bm->write_partial_block(version_block_id, reinterpret_cast<u8 *>(&version),
                                                       version_block_index * sizeof(version_t), sizeof(version_t));
  if (read_res.is_err()) {
    return false;
  }

  return true;
}
} // namespace chfs