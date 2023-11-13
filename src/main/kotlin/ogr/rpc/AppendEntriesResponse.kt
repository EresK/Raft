package ogr.rpc

data class AppendEntriesResponse(
    val term: Int, // currentTerm, for leader to update itself
    val success: Boolean, // true if follower contained entry matching prevLogIndex and prevLogTerm
    val type: RpcType = RpcType.AppendEntriesResponse
)