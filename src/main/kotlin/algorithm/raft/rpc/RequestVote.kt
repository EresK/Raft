package algorithm.raft.rpc

data class RequestVote(
    val term: Int, // candidate's term
    val candidateId: Int,
    val lastLogIndex: Int,
    val lastLogTerm: Int,
    val type: RpcType = RpcType.RequestVote
)