package ogr.util

import ogr.rpc.RpcType
import ogr.transport.TcpConnection

data class ReceivedMessage(
    val connection: TcpConnection,
    val message: String,
    val rpcType: RpcType
)