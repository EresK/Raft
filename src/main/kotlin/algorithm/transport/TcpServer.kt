package algorithm.transport

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*

class TcpServer(
    val self: Node,
    val others: List<Node>,
    private val onNodeConnected: suspend (TcpConnection) -> Unit,
    private val onMessageReceived: suspend (TcpConnection, String) -> Unit
) {
    private val serverSocket: ServerSocket

    companion object {
        private const val ACCEPTING_LOOP_DELAY = 10L
    }

    init {
        serverSocket = aSocket(SelectorManager(Dispatchers.IO)).tcp().bind(self.hostname, self.port)
    }

    private suspend fun accept() {
        val socket = serverSocket.accept()
        val readChannel = socket.openReadChannel()

        try {
            val nodeId = readChannel.readInt()
            val node = others.find { it.id == nodeId }

            if (node == null || self.id == node.id)
                throw IllegalStateException("Accept connection error\nnode: $node\nself id: ${self.id}, node id: $nodeId")

            val connection = TcpConnection(self, node, socket, onMessageReceived, readChannel)
            onNodeConnected(connection)

            println("Accepted node $nodeId") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        }
        catch (e: Exception) {
            System.err.println("Node: ${self.id}. Error while accepting connection")
        }
    }

    fun launchAcceptingLoop(scope: CoroutineScope) {
        val acceptingScope = CoroutineScope(scope.coroutineContext + CoroutineName("Accepting loop"))
        acceptingScope.launch {
            println("Accepting loop $coroutineContext") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            while (!serverSocket.isClosed) {
                accept()
                delay(ACCEPTING_LOOP_DELAY)
            }
            cancel()
        }
    }
}