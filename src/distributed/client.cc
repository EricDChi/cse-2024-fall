#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  inode_id_t inode_id = res.unwrap()->as<inode_id_t>();
  if (inode_id == 0) {
    return ChfsResult<inode_id_t>(ErrorType::INVALID);
  }
  
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto res = metadata_server_->call("unlink", parent, name);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }

  if (!res.unwrap()->as<bool>()) {
    return ChfsNullResult(ErrorType::NotExist);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("lookup", parent, name);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  inode_id_t id = res.unwrap()->as<inode_id_t>();
  if (id == 0) {
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  return ChfsResult<inode_id_t>(res.unwrap()->as<inode_id_t>());
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("readdir", id);
  if (res.is_err()) {
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(res.unwrap_error());
  }

  auto vec = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(vec);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("get_type_attr", id);
  if (res.is_err()) {
    return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
  }

  auto attr = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  InodeType type = static_cast<InodeType>(std::get<4>(attr));
  FileAttr file_attr;
  file_attr.size = std::get<0>(attr);
  file_attr.atime = std::get<1>(attr);
  file_attr.mtime = std::get<2>(attr);
  file_attr.ctime = std::get<3>(attr);

  return ChfsResult<std::pair<InodeType, FileAttr>>({type, file_attr});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  auto get_block_map_res = metadata_server_->call("get_block_map", id);
  if (get_block_map_res.is_err()) {
    return ChfsResult<std::vector<u8>>(get_block_map_res.unwrap_error());
  }
  std::vector<BlockInfo> block_map = get_block_map_res.unwrap()->as<std::vector<BlockInfo>>();

  u32 block_map_size = block_map.size();
  u32 first_block = offset / DiskBlockSize;
  u32 last_block = (offset + size - 1) / DiskBlockSize;
  u32 first_offset = offset % DiskBlockSize;
  u32 last_offset = (offset + size - 1) % DiskBlockSize;

  if (last_block >= block_map_size) {
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID);
  }

  std::vector<u8> result;
  usize read_offset = 0;
  usize read_len = 0;

  // Read data from blocks
  for (u32 i = first_block; i <= last_block; i++) {
    BlockInfo block_info = block_map[i];
    block_id_t block_id = std::get<0>(block_info);
    mac_id_t mac_id = std::get<1>(block_info);
    version_t version = std::get<2>(block_info);
    auto client = data_servers_[mac_id];

    read_offset = 0;
    read_len = DiskBlockSize;
    if (i == first_block) {
      read_offset = first_offset;
      read_len -= first_offset;
    }
    if (i == last_block) {
      read_len = read_len - (DiskBlockSize - last_offset - 1);
    }
    
    auto read_res = client->call("read_data", block_id, read_offset, read_len, version);
    if (read_res.is_err()) {
      return ChfsResult<std::vector<u8>>(read_res.unwrap_error());
    }
    std::vector<u8> data = read_res.unwrap()->as<std::vector<u8>>();
    result.insert(result.end(), data.begin(), data.end());
  }
  return ChfsResult<std::vector<u8>>(result);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto attr_res = metadata_server_->call("get_type_attr", id);
  if (attr_res.is_err()) {
    return ChfsNullResult(attr_res.unwrap_error());
  }
  auto attr = attr_res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  u64 file_size = std::get<0>(attr);
  u64 new_size = offset + data.size();

  // Allocate new blocks if file is not large enough
  while (new_size > file_size) {
    auto alloc_res = metadata_server_->call("alloc_block", id);
    if (alloc_res.is_err()) {
      return ChfsNullResult(alloc_res.unwrap_error());
    }
    file_size += DiskBlockSize;
  }

  auto block_map_res = metadata_server_->call("get_block_map", id);
  if (block_map_res.is_err()) {
    return ChfsNullResult(block_map_res.unwrap_error());
  }
  std::vector<BlockInfo> block_map = block_map_res.unwrap()->as<std::vector<BlockInfo>>();

  // u32 block_map_size = block_map.size();
  u32 first_block = offset / DiskBlockSize;
  u32 last_block = (offset + data.size() - 1) / DiskBlockSize;
  u32 first_offset = offset % DiskBlockSize;
  u32 last_offset = (offset + data.size() - 1) % DiskBlockSize;

  usize write_offset = 0;
  usize write_len = 0;
  usize have_written = 0;

  // Write data to blocks
  for (u32 i = first_block; i <= last_block; i++) {
    BlockInfo block_info = block_map[i];
    block_id_t block_id = std::get<0>(block_info);
    mac_id_t mac_id = std::get<1>(block_info);
    auto client = data_servers_[mac_id];

    write_offset = 0;
    write_len = DiskBlockSize;
    if (i == first_block) {
      write_offset = first_offset;
      write_len -= first_offset;
    }
    if (i == last_block) {
      write_len = write_len - (DiskBlockSize - last_offset - 1);
    }

    std::vector<u8> write_data;
    write_data.insert(write_data.end(), data.begin() + have_written, data.begin() + have_written + write_len);
    auto write_res = client->call("write_data", block_id, write_offset, write_data);
    if (write_res.is_err()) {
      return ChfsNullResult(write_res.unwrap_error());
    }
    have_written += write_len;
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  auto res = metadata_server_->call("free_block", id, block_id, mac_id);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }

  if (!res.unwrap()->as<bool>()) {
    return ChfsNullResult(ErrorType::INVALID);
  }
  return KNullOk;
}

} // namespace chfs