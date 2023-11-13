package algorithm.transport

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.Dispatchers

class SimpleServer(
    val self: Node,
    val others: List<Node>,
    private val onNodeConnected: suspend (SimpleConnection) -> Unit,
    private val onMessageReceived: suspend (SimpleConnection, String) -> Unit
) {
    private val serverSocket: ServerSocket =
        aSocket(SelectorManager(Dispatchers.IO)).tcp().bind(self.hostname, self.port)

    private var isCanceled = false

    private suspend fun accept() {
        val socket = serverSocket.accept()
        val readChannel = socket.openReadChannel()

        try {
            val nodeId = readChannel.readInt()
            val node = others.find { it.id == nodeId }

            if (node == null || self.id == node.id)
                throw IllegalStateException("Accept connection error\nnode: $node\nself id: ${self.id}, node id: $nodeId")

            val connection = SimpleConnection(self, node, socket, onMessageReceived, readChannel)

            onNodeConnected(connection)

            println("Accept $nodeId") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        }
        catch (e: Exception) {
            System.err.println("Node: ${self.id}. Error while accepting connection")
        }
    }

    suspend fun acceptingLoop() {
        while (!isCanceled && !serverSocket.isClosed) {
            accept()
        }
    }

    fun stop() {
        isCanceled = true
        serverSocket.close()
    }
}