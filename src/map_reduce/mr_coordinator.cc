#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, int, int, std::string> Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        std::unique_lock<std::mutex> uniqueLock(this->mtx);

        if (this->isFinished) {
            return std::make_tuple(-1, -1, -1, -1, "");
        }

        if (this->map_finished != this->map_num) {
            for (int i = 0; i < this->map_num; i++) {
                if (this->map_tasks[i] == 0) {
                    this->map_tasks[i] = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                    return std::make_tuple(MAP, i, this->map_num, this->reduce_num, this->files[i]);
                }
                else if (this->map_tasks[i] != -1) {
                    long now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                    if (now - this->map_tasks[i] > 10000) {
                        this->map_tasks[i] = now;
                        return std::make_tuple(MAP, i, this->map_num, this->reduce_num, this->files[i]);
                    }
                } 
            }
            return std::make_tuple(NONE, -1, -1, -1, "");
        }

        if (this->reduce_finished != this->reduce_num) {
            for (int i = 0; i < this->reduce_num; i++) {
                if (this->reduce_tasks[i] == 0) {
                    this->reduce_tasks[i] = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                    return std::make_tuple(REDUCE, i, this->map_num, this->reduce_num, "");
                }
                else if (this->reduce_tasks[i] != -1) {
                    long now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                    if (now - this->reduce_tasks[i] > 10000) {
                        this->reduce_tasks[i] = now;
                        return std::make_tuple(REDUCE, i, this->map_num, this->reduce_num, "");
                    }
                }
            }
            return std::make_tuple(NONE, -1, -1, -1, "");
        }

        if (!this->isMergeAssigned) {
            this->isMergeAssigned = true;
            return std::make_tuple(MERGE, 0, this->map_num, this->reduce_num, "");
        }

        return std::make_tuple(NONE, -1, -1, -1, "");
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        std::unique_lock<std::mutex> uniqueLock(this->mtx);

        if (taskType == mr_tasktype::MAP) {
            this->map_tasks[index] = -1;
            this->map_finished++;
        }
        else if (taskType == mr_tasktype::REDUCE) {
            this->reduce_tasks[index] = -1;
            this->reduce_finished++;
        }
        else if (taskType == mr_tasktype::MERGE) {
            isFinished = true;
            return 0;
        }

        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
        this->isMergeAssigned = false;
        this->map_finished = 0;
        this->reduce_finished = 0;
        this->map_tasks.resize(files.size(), 0);
        this->reduce_tasks.resize(nReduce, 0);
        this->map_num = files.size();
        this->reduce_num = nReduce;
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}