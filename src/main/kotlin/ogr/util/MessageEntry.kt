package ogr.util

import ogr.transport.TcpConnection

data class MessageEntry(
    val connection: TcpConnection,
    val message: String
)