#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

template <typename Command>
struct LogEntry {
    int term;
    Command command;
};

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
    
    MSGPACK_DEFINE(
        term,
        candidate_id,
        last_log_index,
        last_log_term
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int term;
    bool vote_granted;

    MSGPACK_DEFINE(
        term,
        vote_granted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<LogEntry<Command>> log_entries;
    int leader_commit;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<int> term_log_entries;
    std::vector<std::vector<u8>> command_log_entries;
    int leader_commit;

    MSGPACK_DEFINE(
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        term_log_entries,
        command_log_entries,
        leader_commit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    RpcAppendEntriesArgs rpc_arg;

    rpc_arg.term = arg.term;
    rpc_arg.leader_id = arg.leader_id;
    rpc_arg.prev_log_index = arg.prev_log_index;
    rpc_arg.prev_log_term = arg.prev_log_term;
    rpc_arg.leader_commit = arg.leader_commit;
    rpc_arg.term_log_entries = std::vector<int>();
    rpc_arg.command_log_entries = std::vector<std::vector<u8>>();

    for (auto &log_entry : arg.log_entries)
    {
        int term = log_entry.term;
        std::vector<u8> command = log_entry.command.serialize(log_entry.command.size());
        rpc_arg.term_log_entries.push_back(term);
        rpc_arg.command_log_entries.push_back(command);
    }

    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> arg;

    arg.term = rpc_arg.term;
    arg.leader_id = rpc_arg.leader_id;
    arg.prev_log_index = rpc_arg.prev_log_index;
    arg.prev_log_term = rpc_arg.prev_log_term;
    arg.leader_commit = rpc_arg.leader_commit;
    arg.log_entries = std::vector<LogEntry<Command>>();

    for (int i = 0; i < rpc_arg.term_log_entries.size(); i++)
    {
        LogEntry<Command> log_entry;
        log_entry.term = rpc_arg.term_log_entries[i];
        Command command;
        command.deserialize(rpc_arg.command_log_entries[i], rpc_arg.command_log_entries[i].size());
        log_entry.command = command;
        arg.log_entries.push_back(log_entry);
    }

    return arg;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;
    bool success;

    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

} /* namespace chfs */