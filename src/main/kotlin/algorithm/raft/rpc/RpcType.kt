package algorithm.raft.rpc

enum class RpcType {
    AppendEntries,
    AppendEntriesResp,
    RequestVote,
    RequestVoteResp
}