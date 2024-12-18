#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  std::string entry = filename + ':' + inode_id_to_string(id);
  if (src.size() > 0) {
    src += '/';
  }
  src += entry;
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  std::string name, tmp;  // tmp can be either inode id or filename
  if (src.size() == 0) {
    return;
  }

  for (auto c : src) {
    if (c == ':') {
      name = tmp;
      tmp = "";
    } else if (c == '/') {
      list.push_back(DirectoryEntry{name, string_to_inode_id(tmp)});
      tmp = "";
    } else {
      tmp += c;
    }
  }
  list.push_back(DirectoryEntry{name, string_to_inode_id(tmp)});
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for (auto &entry : list) {
    if (entry.name != filename) {
      res = append_to_directory(res, entry.name, entry.id);
    }
  }

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  auto res = fs->read_file(id);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }
  std::string src(reinterpret_cast<const char *>(res.unwrap().data()), res.unwrap().size());
  parse_directory(src, list);

  return KNullOk;
}

auto read_directory_atomic(FileOperation *fs, inode_id_t id,
                          std::list<DirectoryEntry> &list, std::vector<std::shared_ptr<BlockOperation>> &ops) -> ChfsNullResult {
  auto res = fs->read_file_atomic(id, ops);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }
  std::string src(reinterpret_cast<const char *>(res.unwrap().data()), res.unwrap().size());
  parse_directory(src, list);

  return KNullOk;
}
                      
// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  auto res = read_directory(this, id, list);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  for (const auto &entry : list) {
    if (entry.name == name) {
      return ChfsResult<inode_id_t>(entry.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

auto FileOperation::lookup_atomic(inode_id_t id, const char *name,
                                  std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  auto res = read_directory_atomic(this, id, list, ops);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  for (const auto &entry : list) {
    if (entry.name == name) {
      return ChfsResult<inode_id_t>(entry.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {
  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  auto lookup_res = lookup(id, name);
  if (lookup_res.is_ok()) {
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }

  auto inode_id = alloc_inode(type);
  if (inode_id.is_err()) {
    return ChfsResult<inode_id_t>(inode_id.unwrap_error());
  }

  std::list<DirectoryEntry> list;
  auto read_res = read_directory(this, id, list);
  if (read_res.is_err()) {
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  }

  std::string src = dir_list_to_string(list);
  src = append_to_directory(src, name, inode_id.unwrap());
  std::vector<u8> content(src.begin(), src.end());
  auto write_res = write_file(id, content);
  if (write_res.is_err()) {
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }

  return ChfsResult<inode_id_t>(inode_id.unwrap());
}

auto FileOperation::mknode_atomic(inode_id_t id, const char *name, InodeType type,
                                  std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> ChfsResult<inode_id_t> {
  auto lookup_res = lookup_atomic(id, name, ops);
  if (lookup_res.is_ok()) {
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }

  auto inode_id = alloc_inode_atomic(type, ops);
  if (inode_id.is_err()) {
    return ChfsResult<inode_id_t>(inode_id.unwrap_error());
  }

  std::list<DirectoryEntry> list;
  auto read_res = read_directory_atomic(this, id, list, ops);
  if (read_res.is_err()) {
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  }

  std::string src = dir_list_to_string(list);
  src = append_to_directory(src, name, inode_id.unwrap());
  std::vector<u8> content(src.begin(), src.end());
  auto write_res = write_file_atomic(id, content, ops);
  if (write_res.is_err()) {
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }

  return ChfsResult<inode_id_t>(inode_id.unwrap());
}


// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  auto lookup_res = lookup(parent, name);
  if (lookup_res.is_err()) {
    return ChfsNullResult(lookup_res.unwrap_error());
  }
  auto remove_res = remove_file(lookup_res.unwrap());
  if (remove_res.is_err()) {
    return ChfsNullResult(remove_res.unwrap_error());
  }

  std::list<DirectoryEntry> list;
  read_directory(this, parent, list);
  std::string src = dir_list_to_string(list);
  src = rm_from_directory(src, name);
  std::vector<u8> content(src.begin(), src.end());
  auto write_res = write_file(parent, content);
  if (write_res.is_err()) {
    return ChfsNullResult(write_res.unwrap_error());
  } 
  
  return KNullOk;
}

auto FileOperation::unlink_atomic(inode_id_t parent, const char *name,
                                  std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> ChfsNullResult {
  auto lookup_res = lookup_atomic(parent, name, ops);
  if (lookup_res.is_err()) {
    return ChfsNullResult(lookup_res.unwrap_error());
  }
  auto remove_res = remove_file_atomic(lookup_res.unwrap(), ops);
  if (remove_res.is_err()) {
    return ChfsNullResult(remove_res.unwrap_error());
  }

  std::list<DirectoryEntry> list;
  read_directory_atomic(this, parent, list, ops);
  std::string src = dir_list_to_string(list);
  src = rm_from_directory(src, name);
  std::vector<u8> content(src.begin(), src.end());
  auto write_res = write_file_atomic(parent, content, ops);
  if (write_res.is_err()) {
    return ChfsNullResult(write_res.unwrap_error());
  }

  return KNullOk;
}

} // namespace chfs
