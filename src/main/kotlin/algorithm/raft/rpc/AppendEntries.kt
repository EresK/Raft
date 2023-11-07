package algorithm.raft.rpc

data class AppendEntries(
    val term: Int, // leader's term
    val leaderId: Int,
    val prevLogIndex: Int,
    val prevLogTerm: Int,
    val entries: List<String>,
    val leaderCommit: Int, // leader's commit index
    val type: RpcType = RpcType.AppendEntries
)