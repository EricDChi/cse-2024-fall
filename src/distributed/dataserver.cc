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

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
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
<<<<<<< HEAD
  UNIMPLEMENTED();

  return {};
=======
  if (offset + len > block_allocator_->bm->block_size()) {
    return std::vector<u8> {};
  }

  std::vector<u8> buffer(block_allocator_->bm->block_size());

  const auto total_version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
  auto version_block_id = block_id / total_version_per_block;
  auto version_block_index = block_id % total_version_per_block;

  block_allocator_->bm->read_block(version_block_id, buffer.data());
  auto res = *reinterpret_cast<version_t *>(buffer.data() + version_block_index * sizeof(version_t));

  if (res.is_err() || res.unwrap() != version) {
    return std::vector<u8> {};
  }

  res = block_allocator_->bm->read_block(block_id, buffer.data());
  if 
  std::vector<u8> result(buffer.begin() + offset, buffer.begin() + offset + len);

  return result;
>>>>>>> lab1
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
<<<<<<< HEAD
  UNIMPLEMENTED();

  return false;
=======
  if (buffer.size() + offset > block_allocator_->bm->block_size()) {
    return false;
  }

  auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(),
                                                       offset, buffer.size());
  if (res.is_err()) {
    return false;
  }

  return true;
>>>>>>> lab1
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
<<<<<<< HEAD
  UNIMPLEMENTED();

  return {};
=======
  auto res = block_allocator_->alloc_block();
  if (res.is_err()) {
    return {KInvalidBlockID, KInvalidVersion};
  }

  block_id_t block_id = res.unwrap();
  version_t version = 0;

  const auto total_version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
  auto version_block_id = block_id / total_version_per_block;
  auto version_block_index = block_id % total_version_per_block;
  std::vector<u8> buffer(block_allocator_->bm->block_size());

  res = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (res.is_err()) {
    return {KInvalidBlockID, KInvalidVersion};
  }

  version = *reinterpret_cast<version_t *>(buffer.data() + version_block_index * sizeof(version_t));
  version += 1;
  res = block_allocator_->bm->write_partial_block(version_block_id, reinterpret_cast<u8 *>(&version),
                                                  version_block_index * sizeof(version_t), sizeof(version_t));
  if (res.is_err()) {
    return {KInvalidBlockID, KInvalidVersion};
  }

  return {block_id, version};
>>>>>>> lab1
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
<<<<<<< HEAD
  UNIMPLEMENTED();

  return false;
=======
  auto res = block_allocator_->deallocate(block_id);
  if (res.is_err()) {
    return false;
  }

  version_t version = 0;

  const auto total_version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
  auto version_block_id = block_id / total_version_per_block;
  auto version_block_index = block_id % total_version_per_block;
  std::vector<u8> buffer(block_allocator_->bm->block_size());

  res = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (res.is_err()) {
    return false;
  }

  version = *reinterpret_cast<version_t *>(buffer.data() + version_block_index * sizeof(version_t));
  version += 1;
  res = block_allocator_->bm->write_partial_block(version_block_id, reinterpret_cast<u8 *>(&version),
                                                  version_block_index * sizeof(version_t), sizeof(version_t));
  if (res.is_err()) {
    return false;
  }

  return true;
>>>>>>> lab1
}
} // namespace chfs