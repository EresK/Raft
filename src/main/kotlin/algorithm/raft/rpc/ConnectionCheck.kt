package algorithm.raft.rpc

data class ConnectionCheck(
    val type: RpcType = RpcType.ConnectionCheck
)