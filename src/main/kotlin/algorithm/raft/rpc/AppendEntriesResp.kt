package algorithm.raft.rpc

data class AppendEntriesResp(
    val term: Int, // currentTerm, for leader to update itself
    val success: Boolean, // true if follower contained entry matching prevLogIndex and prevLogTerm
    val type: RpcType = RpcType.AppendEntriesResp
)