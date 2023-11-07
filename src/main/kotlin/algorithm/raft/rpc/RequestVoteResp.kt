package algorithm.raft.rpc

data class RequestVoteResp(
    val term: Int, // currentTerm, for candidate to update itself
    val voteGranted: Boolean, // true means candidate received vote
    val type: RpcType = RpcType.RequestVoteResp
)