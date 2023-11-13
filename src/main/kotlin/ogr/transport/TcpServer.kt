package ogr.transport

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.Dispatchers

class TcpServer(
    val self: Node,
    val others: List<Node>,
    private val onNodeConnected: suspend (TcpConnection) -> Unit,
    private val onMessageReceived: suspend (TcpConnection, String) -> Unit
) {
    private val serverSocket: ServerSocket =
        aSocket(SelectorManager(Dispatchers.IO)).tcp().bind(self.hostname, self.port)

    private var isCanceled = false

    suspend fun acceptingLoop() {
        while (!isCanceled) {
            accept()
        }
    }

    private suspend fun accept() {
        val socket = serverSocket.accept()
        val readChannel = socket.openReadChannel()

        try {
            val nodeId = readChannel.readInt()
            val node = others.find { it.id == nodeId }

            if (node == null || self.id == node.id)
                throw IllegalStateException(
                    "Accept connection error\n" +
                            "node: $node\n" +
                            "self id: ${self.id}, node id: $nodeId"
                )

            val connection = TcpConnection(self, node, socket, onMessageReceived, readChannel)

            onNodeConnected(connection)

            println("Accept $nodeId") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        }
        catch (e: Exception) {
            System.err.println("Accept error, server stopped")
            stop()
        }
    }

    fun stop() {
        isCanceled = true
        serverSocket.close()
    }
}