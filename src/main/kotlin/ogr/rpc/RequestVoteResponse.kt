package ogr.rpc

data class RequestVoteResponse(
    val term: Int, // currentTerm, for candidate to update itself
    val voteGranted: Boolean, // true means candidate received vote
    val type: RpcType = RpcType.RequestVoteResponse
)