#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;

    /* Transform physical index to logical index */
    auto physical_2_logical(int physical_index) -> int;
    /* Transform logical index to physical index */
    auto logical_2_physical(int logical_index) -> int;

    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    // for all servers
    int voted_for;
    long last_heartbeat;

    std::vector<LogEntry<Command>> log_entries;

    int commit_index;
    int last_applied;

    // for candidates
    int voted_count;

    // for leader
    std::vector<int> next_index;
    std::vector<int> match_index;

    /* for snapshot */
    std::vector<u8> snapshot_data;
    int last_included_index;
    int last_included_term;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

    /* Lab3: Your code here */ 
    state = std::make_unique<StateMachine>();
    thread_pool = std::make_unique<ThreadPool>(configs.size());

    last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    next_index.resize(configs.size(), 0);
    match_index.resize(configs.size(), 0);

    voted_count = 0;

    std::string log_path = "/tmp/raft_log/node_" + std::to_string(my_id);
    bool is_initialized = is_file_exist(log_path);
    if (!is_initialized) {
        log_storage = std::make_unique<RaftLog<Command>>(log_path);
        current_term = 0;
        voted_for = -1;
        commit_index = 0;
        last_applied = 0;
        LogEntry<Command> entry;
        entry.term = 0;
        log_entries.push_back(entry);
        last_included_index = 0;
        last_included_term = 0;
        snapshot_data = state->snapshot();

        log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
        log_storage->persist_log(log_entries);
        log_storage->persist_snapshot(last_included_index, last_included_term, snapshot_data);
    } else {
        log_storage = std::make_unique<RaftLog<Command>>(log_path);
        bool recover_metadata_res = log_storage->recover_metadata(current_term, voted_for, commit_index, last_applied);
        if (!recover_metadata_res) {
            RAFT_LOG("Node %d recover metadata failed", my_id);
        }

        bool recover_log_res = log_storage->recover_log(log_entries);
        if (!recover_log_res) {
            RAFT_LOG("Node %d recover log failed", my_id);
        }

        bool recover_snapshot_res = log_storage->recover_snapshot(last_included_index, last_included_term, snapshot_data);
        if (!recover_snapshot_res) {
            RAFT_LOG("Node %d recover snapshot failed", my_id);
        }
        last_applied = last_included_index;
        state->apply_snapshot(snapshot_data);
    }

    // RAFT_LOG("Node %d created", my_id);
    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */
    stopped.store(false);

    for (int i = 0; i < node_configs.size(); i++) {
        int node_id = node_configs[i].node_id;
        rpc_clients_map.insert(std::make_pair(node_id,
            std::make_unique<RpcClient>(node_configs[i].ip_address, node_configs[i].port, true)));
    }

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    stopped.store(true);

    if (background_election) {
        background_election->join();
    }
    if (background_ping) {
        background_ping->join();
    }
    if (background_commit) {
        background_commit->join();
    }
    if (background_apply) {
        background_apply->join();
    }

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    if (role == RaftRole::Leader) {
        return std::make_tuple(true, current_term);
    }

    return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    if (role != RaftRole::Leader) {
        return std::make_tuple(false, current_term, physical_2_logical(log_entries.size() - 1));
    }

    LogEntry<Command> log_entry;
    log_entry.term = current_term;
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);
    log_entry.command = cmd;
    log_entries.push_back(log_entry);
    // persist log
    log_storage->persist_log(log_entries);

    // RAFT_LOG("Node %d received new command term %d log index %d", my_id, current_term, physical_2_logical(log_entries.size() - 1));

    return std::make_tuple(true, current_term, physical_2_logical(log_entries.size() - 1));
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::physical_2_logical(int physical_index) -> int
{
    return physical_index + last_included_index;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::logical_2_physical(int logical_index) -> int
{
    return logical_index - last_included_index;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    std::unique_lock<std::mutex> lock(mtx);

    // apply all commited log entries
    for (int i = logical_2_physical(last_applied) + 1; i <= logical_2_physical(commit_index); i++) {
        state->apply_log(log_entries[i].command);
    }

    // update metadata
    last_applied = commit_index;
    snapshot_data = state->snapshot();

    // update log entries
    std::vector<LogEntry<Command>> new_log_entries;
    for (int i = logical_2_physical(last_applied); i < log_entries.size(); i++) {
        new_log_entries.push_back(log_entries[i]);
    }
    log_entries = new_log_entries;

    last_included_term = log_entries[logical_2_physical(last_applied)].term;
    last_included_index = last_applied;
    log_entries[0].term = last_included_term;

    // persist snapshot
    log_storage->persist_snapshot(last_included_index, last_included_term, snapshot_data);
    // persist log
    log_storage->persist_log(log_entries);

    // RAFT_LOG("Node %d saved snapshot with last_included_index %d last_included_term %d", my_id, last_included_index, last_included_term);

    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return this->get_snapshot_direct();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    // RAFT_LOG("Node %d trying to request vote from node %d", args.candidate_id, my_id);

    RequestVoteReply reply;

    // if term < currentTerm
    if (args.term < current_term) {
        // RAFT_LOG("Node %d received request vote from node %d with term %d < current term %d", my_id, args.candidate_id, args.term, current_term);
        reply.term = current_term;
        reply.vote_granted = false;
        return reply;
    }

    // check for votedFor
    if (current_term == args.term && voted_for != -1 && voted_for != args.candidate_id) {
        // RAFT_LOG("Node %d received request vote from node %d but already voted for node %d", my_id, args.candidate_id, voted_for);
        reply.term = current_term;
        reply.vote_granted = false;
        return reply;
    }

    role = RaftRole::Follower;
    current_term = args.term;
    voted_for = -1;
    // check for up-to-date
    int last_log_term = log_entries[log_entries.size() - 1].term;
    if (args.last_log_term < last_log_term || 
        (args.last_log_term == last_log_term && args.last_log_index < physical_2_logical(log_entries.size() - 1))) {
        // RAFT_LOG("Node %d received request vote from node %d but not up-to-date", my_id, args.candidate_id);
        reply.term = current_term;
        reply.vote_granted = false;
        // persist metadata
        log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
        return reply;
    }

    // grant vote
    // RAFT_LOG("Node %d granted vote to node %d", my_id, args.candidate_id);
    voted_for = args.candidate_id;
    reply.term = current_term;
    reply.vote_granted = true;
    leader_id = args.candidate_id;
    // persist metadata
    log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);

    last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    // RAFT_LOG("Node %d received vote? from node %d", my_id, target);

    last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    // if RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower
    if (reply.term > current_term) {
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        // persist metadata
        log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
        return;
    }

    // if response contains term T < currentTerm or not candidate anymore
    if (role != RaftRole::Candidate || reply.term < current_term) {
        return;
    }

    // if vote not granted
    if (!reply.vote_granted) {
        return;
    }

    voted_count++;
    if (voted_count > node_configs.size() / 2) {
        role = RaftRole::Leader;
        // RAFT_LOG("Node %d become leader", my_id);
        for (int i = 0; i < node_configs.size(); i++) {
            next_index[i] = physical_2_logical(log_entries.size());
            match_index[i] = 0;
        }
    }

    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    AppendEntriesReply reply;
    AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);

    // check if metadata or log is changed
    bool is_metadata_changed = false;
    bool is_log_changed = false;

    // if term < currentTerm
    if (arg.term < current_term) {
        reply.term = current_term;
        reply.success = false;
        return reply;
    }

    role = RaftRole::Follower;
    current_term = arg.term;    
    voted_for = -1;
    leader_id = arg.leader_id;
    is_metadata_changed = true;

    last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    // if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    if (logical_2_physical(arg.prev_log_index) > log_entries.size() - 1 || 
        (logical_2_physical(arg.prev_log_index) >= 0 && log_entries[logical_2_physical(arg.prev_log_index)].term != arg.prev_log_term)) {
        reply.term = current_term;
        // RAFT_LOG("Node %d received append entries from node %d but log doesn't contain an entry at prevLogIndex %d", my_id, arg.leader_id, arg.prev_log_index);
        reply.success = false;
        // persist metadata
        log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
        return reply;
    }

    // if an existing entry conflicts with a new one (same index but different terms)
    if (arg.log_entries.size() > 0) {
        // RAFT_LOG("Node %d received append entries from node %d with log entries", my_id, arg.leader_id);
        int index = logical_2_physical(arg.prev_log_index);
        for (int i = 0; i < arg.log_entries.size(); i++) {
            index++;
            if (index < log_entries.size() && log_entries[index].term != arg.log_entries[i].term) {
                log_entries.erase(log_entries.begin() + index, log_entries.end());
                is_log_changed = true;
            }

            if (index >= log_entries.size()) {
                log_entries.push_back(arg.log_entries[i]);
                is_log_changed = true;
            }
        }
    }

    // if leaderCommit > commitIndex
    // set commitIndex = min(leaderCommit, index of last new entry)
    int last_new_entry_index = physical_2_logical(log_entries.size() - 1);
    if (arg.leader_commit > commit_index) {
        if (arg.leader_commit < last_new_entry_index) {
            commit_index = arg.leader_commit;
        } else {
            commit_index = last_new_entry_index;
        }
    }

    reply.term = current_term;
    reply.success = true;

    // persist metadata
    if (is_metadata_changed) {
        log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
    }
    // persist log
    if (is_log_changed) {
        log_storage->persist_log(log_entries);
    }

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    // RAFT_LOG("Node %d received append entries reply from node %d", my_id, node_id);

    last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    // if RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower
    if (reply.term > current_term) {
        current_term = reply.term;
        role = RaftRole::Follower;
        voted_for = -1;
        // persist metadata
        log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
        return;
    }

    if  (role != RaftRole::Leader) {
        return;
    }

    if (reply.success) {
        if (arg.log_entries.size() + arg.prev_log_index > match_index[node_id]) {
            match_index[node_id] = arg.log_entries.size() + arg.prev_log_index;
        }
        next_index[node_id] = match_index[node_id] + 1;

        // update commitIndex
        for (int N = physical_2_logical(log_entries.size() - 1); N > commit_index; N--) {
            int count = 1;
            if (log_entries[logical_2_physical(N)].term != current_term) {
                continue;
            }

            for (auto map_it = rpc_clients_map.begin(); map_it != rpc_clients_map.end(); map_it++) {
                int target_id = map_it->first;
                if (target_id == my_id) {
                    continue;
                }
                
                if (match_index[target_id] >= N) {
                    count++;
                }

                if (count > node_configs.size() / 2) {
                    commit_index = N;
                    log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
                    break;
                }
            }

            if (commit_index == N) {
                break;
            }
        }
    } else {
        next_index[node_id]--;
    }

    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    InstallSnapshotReply reply;

    // if term < currentTerm
    if (args.term < current_term) {
        reply.term = current_term;
        return reply;
    }
    
    role = RaftRole::Follower;
    current_term = args.term;
    voted_for = -1;
    leader_id = args.leader_id;

    // if existing log entry has same index and term as snapshot's last included entry
    // retain log entries following the snapshot
    if (logical_2_physical(args.last_included_index) < log_entries.size() &&
        log_entries[logical_2_physical(args.last_included_index)].term == args.last_included_term) {
        log_entries.erase(log_entries.begin() + 1, log_entries.begin() + logical_2_physical(args.last_included_index) + 1);
        log_entries[0].term = args.last_included_term;
    }
    else {
        log_entries.clear();
        LogEntry<Command> entry;
        entry.term = args.last_included_term;
        log_entries.push_back(entry);
    }
    snapshot_data = args.data;
    last_included_index = args.last_included_index;
    last_included_term = args.last_included_term;
    state->apply_snapshot(snapshot_data);
    last_applied = last_included_index;
    if (commit_index < last_included_index) {
        commit_index = last_included_index;
    }
    
    log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
    log_storage->persist_log(log_entries);
    log_storage->persist_snapshot(last_included_index, last_included_term, snapshot_data);

    // RAFT_LOG("Node %d installed snapshot with last_included_index %d last_included_term %d", my_id, last_included_index, last_included_term);

    reply.term = current_term;
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    // if RPC request or response contains term T > currentTerm:    
    // set currentTerm = T, convert to follower
    if (reply.term > current_term) {
        current_term = reply.term;
        role = RaftRole::Follower;
        voted_for = -1;
        // persist metadata
        log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
        return;
    }

    if (role != RaftRole::Leader) {
        return;
    }

    if (match_index[node_id] < arg.last_included_index) {
        match_index[node_id] = arg.last_included_index;
    }
    next_index[node_id] = match_index[node_id] + 1;

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();

            // generate a random election timeout from 150ms to 300ms
            auto election_timeout_milli = std::chrono::milliseconds(150 + rand() % 150);
            long election_timeout = election_timeout_milli.count();
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

            if ((role == RaftRole::Follower && now - last_heartbeat > election_timeout) ||
                (role == RaftRole::Candidate && now - last_heartbeat > election_timeout)) {
                // RAFT_LOG("Node %d start election", my_id);
                role = RaftRole::Candidate;
                current_term++;
                voted_for = my_id;
                voted_count = 1;
                last_heartbeat = now;

                RequestVoteArgs args;
                args.term = current_term;
                args.candidate_id = my_id;
                args.last_log_index = physical_2_logical(log_entries.size() - 1);
                args.last_log_term = log_entries[log_entries.size() - 1].term;

                for (auto map_it = rpc_clients_map.begin(); map_it != rpc_clients_map.end(); map_it++) {
                    int target_id = map_it->first;
                    if (target_id == my_id) {
                        continue;
                    }
                    thread_pool->enqueue(&RaftNode::send_request_vote, this, target_id, args);
                }

                // persist metadata
                log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
            }

            mtx.unlock();

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();

            if (role != RaftRole::Leader) {
                mtx.unlock();
                continue;
            }

            for (auto map_it = rpc_clients_map.begin(); map_it != rpc_clients_map.end(); map_it++) {
                int target_id = map_it->first;
                if (target_id == my_id) {
                    continue;
                }

                if (next_index[target_id] >= physical_2_logical(log_entries.size())) {
                    continue;  
                }

                if (logical_2_physical(next_index[target_id]) <= 0) {
                    // send snapshot
                    InstallSnapshotArgs args;
                    args.term = current_term;
                    args.leader_id = my_id;
                    args.last_included_index = last_included_index;
                    args.last_included_term = last_included_term;
                    args.offset = 0;
                    args.data = snapshot_data;
                    args.done = true;

                    thread_pool->enqueue(&RaftNode::send_install_snapshot, this, target_id, args);

                    continue;
                }

                AppendEntriesArgs<Command> args;
                args.term = current_term;
                args.leader_id = my_id;
                args.prev_log_index = next_index[target_id] - 1;
                args.prev_log_term = log_entries[logical_2_physical(args.prev_log_index)].term;
                args.log_entries = std::vector<LogEntry<Command>>();
                args.leader_commit = commit_index;

                for (int i = logical_2_physical(next_index[target_id]); i < log_entries.size(); i++) {
                    args.log_entries.push_back(log_entries[i]);
                }

                if (args.log_entries.size() > 0) {
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, target_id, args);
                }
            }

            mtx.unlock();

            std::this_thread::sleep_for(std::chrono::milliseconds(40));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();

            if (last_applied < commit_index) {
                last_applied++;
                for (int i = logical_2_physical(last_applied); i <= logical_2_physical(commit_index); i++) {
                    // RAFT_LOG("Node %d apply log %d", my_id, i);
                    state->apply_log(log_entries[i].command);
                }
                last_applied = commit_index;
                // persist metadata
                log_storage->persist_metadata(current_term, voted_for, commit_index, last_applied);
            }

            mtx.unlock();

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();

            if (role == RaftRole::Leader) {
                AppendEntriesArgs<Command> args;
                args.term = current_term;
                args.leader_id = my_id;
                args.log_entries = std::vector<LogEntry<Command>>();
                args.leader_commit = commit_index;

                for (auto map_it = rpc_clients_map.begin(); map_it != rpc_clients_map.end(); map_it++) {
                    int target_id = map_it->first;
                    args.prev_log_index = next_index[target_id] - 1;
                    args.prev_log_term = log_entries[logical_2_physical(args.prev_log_index)].term;
                    if (target_id == my_id) {
                        continue;
                    }
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, target_id, args);
                }
            }

            mtx.unlock();

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}