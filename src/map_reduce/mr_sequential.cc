#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<KeyVal> key_vals;

        for (auto &file : files) {
            auto lookup_res = chfs_client->lookup(1, file);
            if (lookup_res.is_err()) {
                return;
            }
            auto id = lookup_res.unwrap();

            auto type_attr_res = chfs_client->get_type_attr(id);
            if (type_attr_res.is_err()) {
                return;
            }
            auto type_attr = type_attr_res.unwrap();
            auto size = type_attr.second.size;

            auto read_res = chfs_client->read_file(id, 0, size);
            if (read_res.is_err()) {
                return;
            }
            auto content = read_res.unwrap();
            std::string content_str(content.begin(), content.end());
            std::vector<KeyVal> key_val = Map(content_str);
            key_vals.insert(key_vals.end(), key_val.begin(), key_val.end());
        }

        std::sort(key_vals.begin(), key_vals.end(), [](const KeyVal &a, const KeyVal &b) {
            return a.key < b.key;
        });

        auto lookup_output_res = chfs_client->lookup(1, outPutFile);
        if (lookup_output_res.is_err()) {
            return;
        }
        auto output_id = lookup_output_res.unwrap();
        std::vector<std::string> values;
        std::string key = key_vals[0].key;
        std::string write_str = "";
        int offset = 0;
        for (auto &key_val : key_vals) {
            if (key_val.key == key) {
                values.push_back(key_val.val);
            } else {
                std::string reduce_res = Reduce(key, values);
                write_str = key + " " + reduce_res + " ";
                chfs_client->write_file(output_id, offset, std::vector<chfs::u8>(write_str.begin(), write_str.end()));
                offset += write_str.size();
                key = key_val.key;
                values.clear();
                values.push_back(key_val.val);
            }
        }
        if (!values.empty()) {
            std::string reduce_res = Reduce(key, values);
            write_str = key + " " + reduce_res + " ";
            chfs_client->write_file(output_id, offset, std::vector<chfs::u8>(write_str.begin(), write_str.end()));
            offset += write_str.size();
        }
    }
}