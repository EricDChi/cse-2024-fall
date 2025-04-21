#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
        auto lookup_res = chfs_client->lookup(1, filename);
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
        std::vector<KeyVal> key_vals = Map(content_str);

        std::sort (key_vals.begin(), key_vals.end(), [](const KeyVal &a, const KeyVal &b) {
            return a.key < b.key;
        });

        std::string result = "";
        for (auto &key_val : key_vals) {
            result += key_val.key + " " + key_val.val + " ";
        }
        std::vector<chfs::u8> result_vec(result.begin(), result.end());

        std::string output_file = "mr-map-" + std::to_string(index);
        auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, output_file);
        if (mknode_res.is_err()) {
            return;
        }
        auto write_res = chfs_client->write_file(mknode_res.unwrap(), 0, result_vec);
        if (write_res.is_err()) {
            return;
        }
    }

    void Worker::doReduce(int index, int nfiles, int nReduce) {
        // Lab4: Your code goes here.
        std::vector<KeyVal> key_vals;

        for (int i = 0; i < nfiles; i++) {
            std::string filename = "mr-map-" + std::to_string(i);
            auto lookup_res = chfs_client->lookup(1, filename);
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
            std::stringstream stringstream(content_str);
            std::string key, value;
            while (stringstream >> key >> value) {
                key_vals.push_back(KeyVal(key, value));
            }
        }

        std::sort(key_vals.begin(), key_vals.end(), [](const KeyVal &a, const KeyVal &b) {
            return a.key < b.key;
        });

        std::string output_file = "mr-reduce-" + std::to_string(index);
        auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, output_file);
        if (mknode_res.is_err()) {
            return;
        }
        auto id = mknode_res.unwrap();

        int i = key_vals.size() / nReduce * index;
        int j = key_vals.size() / nReduce * (index + 1);
        if (index == nReduce - 1) {
            j = key_vals.size();
        }
        if (i != 0) {
            std::string key = key_vals[i - 1].key;
            while (i < key_vals.size() && key_vals[i].key == key) {
                i++;
            }
        }
        std::vector<std::string> values;
        std::string key = key_vals[i].key;
        std::string write_str = "";
        int offset = 0;
        for (; i < j; i++) {
            if (key_vals[i].key == key) {  
                values.push_back(key_vals[i].val);
            } else {
                std::string reduce_res = Reduce(key, values);
                write_str = key + " " + reduce_res + " ";
                chfs_client->write_file(id, offset, std::vector<chfs::u8>(write_str.begin(), write_str.end()));
                offset += write_str.size();
                key = key_vals[i].key;
                values.clear();
                values.push_back(key_vals[i].val);
            }
        }
        if (!values.empty()) {
            for (; i < key_vals.size(); i++) {
                if (key_vals[i].key == key) {
                    values.push_back(key_vals[i].val);
                } else {
                    break;
                }
            }
            std::string reduce_res = Reduce(key, values);
            write_str = key + " " + reduce_res + " ";
            chfs_client->write_file(id, offset, std::vector<chfs::u8>(write_str.begin(), write_str.end()));
            offset += write_str.size();
        }
    }

    void Worker::doMerge(int nReduce) {
        auto lookup_res = chfs_client->lookup(1, outPutFile);
        if (lookup_res.is_err()) {
            return;
        }
        auto output_id = lookup_res.unwrap();
        int offset = 0;

        for (int i = 0; i < nReduce; i++) {
            std::string filename = "mr-reduce-" + std::to_string(i);
            auto lookup_res = chfs_client->lookup(1, filename);
            if (lookup_res.is_err()) {
                return;
            }
            auto file_id = lookup_res.unwrap();

            auto type_attr_res = chfs_client->get_type_attr(file_id);
            if (type_attr_res.is_err()) {
                return;
            }
            auto type_attr = type_attr_res.unwrap();
            auto size = type_attr.second.size;

            auto read_res = chfs_client->read_file(file_id, 0, size);
            if (read_res.is_err()) {
                return;
            }
            auto content = read_res.unwrap();
            auto write_res = chfs_client->write_file(output_id, offset, content);
            if (write_res.is_err()) {
                return;
            }
            offset += content.size();
        }
        doSubmit(mr_tasktype::NONE, 0);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            auto ask_res = mr_client->call(ASK_TASK, 0);
            if (ask_res.is_err()) {
                return;
            }

            auto task = ask_res.unwrap()->as<std::tuple<int, int, int, int, std::string>>();
            int taskType = std::get<0>(task);
            int index = std::get<1>(task);
            int nMap = std::get<2>(task);
            int nReduce = std::get<3>(task);
            std::string filename = std::get<4>(task);

            if (taskType == mr_tasktype::MAP) {
                doMap(index, filename);
                doSubmit(mr_tasktype::MAP, index);
            } else if (taskType == mr_tasktype::REDUCE) {
                doReduce(index, nMap, nReduce);
                doSubmit(mr_tasktype::REDUCE, index);
            } else if (taskType == mr_tasktype::MERGE) {
                doMerge(nReduce);
                doSubmit(mr_tasktype::MERGE, 0);
            }

            if (taskType == mr_tasktype::NONE) {
                continue;
            }

            if (taskType == -1) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}