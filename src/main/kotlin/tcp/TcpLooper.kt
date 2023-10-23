package tcp

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

class TcpLooper(private val self: Node, private val others: List<Node>) {
    companion object {
        private const val TRY_DELAY = SECOND
        private const val CANCELLATION_DELAY = 3 * SECOND
        private val connectionCheckDelta: Duration = (10).seconds
    }

    private lateinit var selfSocket: ServerSocket
    private val tcpConnMap: MutableMap<Node, TcpConnection> = mutableMapOf()
    private val tcpMutex = Mutex()

    private val acceptingAtomic = AtomicBoolean(false)
    private var cancelled = false

    var onNodeConnectedCallback: (TcpConnection) -> Unit = {}

    suspend fun start() {
        selfSocket = aSocket(SelectorManager(Dispatchers.IO)).tcp().bind(self.hostname, self.port)
        maintainConnections()
    }

    fun stop() {
        cancelled = true
    }

    private suspend fun release() {
        withContext(Dispatchers.IO) {
            selfSocket.close()
            println(" - Server socket closed") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        }

        tcpMutex.withLock {
            for (connection in tcpConnMap.values)
                connection.close()
        }
        println(" - Resources released") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    }

    private suspend fun maintainConnections() {
        println("Server started >>>") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        val connJob = Job()
        val connScope = CoroutineScope(Dispatchers.IO + connJob)

        while (!cancelled) {
            for (node in others) {
                val connection = tcpMutex.withLock { tcpConnMap[node] }

                if (connection == null || connection.status == TcpStatus.DISCONNECTED) {
                    if (!shouldConnect(node) && !acceptingAtomic.getAndSet(true)) {
                        acceptNode(connScope)
                    }
                    else if (shouldConnect(node)) {
                        connectToNode(node, connScope)
                    }
                }
                else if (connection.status == TcpStatus.CONNECTED &&
                    connection.lastConnectionCheckTime + connectionCheckDelta < TimeSource.Monotonic.markNow()) {
                    connection.writeMessage("{type: \"connection check\"}")
                }
            }
            delay(TRY_DELAY)
        }

        println("Server is stopping...") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        connScope.cancel()
        release()
        delay(CANCELLATION_DELAY)

        println("<<< Server stopped") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    }

    /**
     * @return false if self id less than node id and true if self id greater than node id
     * @throws NodesRepeatIdException if self and node ids are equal
     */
    private fun shouldConnect(node: Node): Boolean {
        return if (self.id < node.id) false
        else if (self.id > node.id) true
        else throw NodesRepeatIdException("Nodes ids must be unique, self id: ${self.id}, node id: ${node.id}")
    }

    /**
     * @param scope coroutine scope to launch connection accepting
     * @throws IllegalStateException if not found node with specific id in "others" nodes, or self node id is equal to node id
     */
    private suspend fun acceptNode(scope: CoroutineScope) {
        scope.launch {
            println("Accepting...") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            val socket = selfSocket.accept()
            val writeChannel = socket.openWriteChannel(autoFlush = true)
            val readChannel = socket.openReadChannel()

            try {
                // reading other node id to resolve connection source
                val nodeId = readChannel.readInt()
                val node = others.find { it.id == nodeId }

                if (node == null || self.id == node.id)
                    throw IllegalStateException("Accept connection error\nnode: $node\nself id: ${self.id}, node id: $nodeId")

                val connection = TcpConnection(self, node, socket, writeChannel, readChannel)
                tcpMutex.withLock { tcpConnMap[node] = connection }

                onNodeConnectedCallback(connection)

                println("Accepted node $nodeId!") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            }
            catch (e: Exception) {
                System.err.println("Node: ${self.id}. Error while accepting connection")
                socket.close()
            }

            acceptingAtomic.set(false) // sets value of atomic boolean to false, aka releases handler for new connections
        }
    }

    /**
     * @param node other node which id must be greater than self node id
     * @param scope
     * @throws IllegalArgumentException
     */
    private suspend fun connectToNode(node: Node, scope: CoroutineScope) {
        if (self.id <= node.id)
            throw IllegalArgumentException("Node id must be greater than self node id")

        scope.launch {
            try {
                println("Connecting...") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                val socket = aSocket(SelectorManager(Dispatchers.IO)).tcp().connect(node.hostname, node.port)
                val writeChannel = socket.openWriteChannel(autoFlush = true)
                val readChannel = socket.openReadChannel()

                // write own id, it helps for accepting node to recognize current node
                writeChannel.writeInt(self.id)

                val connection = TcpConnection(self, node, socket, writeChannel, readChannel)
                tcpMutex.withLock { tcpConnMap[node] = connection }

                onNodeConnectedCallback(connection)

                println("Connected to ${node.id}!") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            }
            catch (e: Exception) {
                System.err.println("Node: ${self.id}. Can not connect to node: ${node.id}")
            }
        }
    }
}