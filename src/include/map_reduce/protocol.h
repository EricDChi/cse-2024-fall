#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

//Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal(){}
        std::string key;
        std::string val;
    };

    enum mr_tasktype {
        NONE = 0,
        MAP,
        REDUCE,
        MERGE
    };

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        std::tuple<int, int, int, int, std::string> askTask(int);
        int submitTask(int taskType, int index);
        bool Done();

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        bool isMergeAssigned;
        std::unique_ptr<chfs::RpcServer> rpc_server;

        std::vector<long> map_tasks;
        std::vector<long> reduce_tasks;
        int map_finished;
        int reduce_finished;
        int map_num;
        int reduce_num;
    };

    class Worker {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles, int nReduce);
        void doSubmit(mr_tasktype taskType, int index);
        void doMerge(int nReduce);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;
    };
}