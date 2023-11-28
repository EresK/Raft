package ogr.transport

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*
import ogr.util.collection.CoroutineMap
import tcp.NodesRepeatIdException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class TcpLooper(
    val self: Node,
    val others: List<Node>,
    private val onNodeConnected: suspend (TcpConnection) -> Unit,
    private val onMessageReceived: suspend (TcpConnection, String) -> Unit,
    private val checkConnection: suspend (TcpConnection) -> Unit
) {
    private val server = TcpServer(self, others, this::onTcpLooperNodeConnected, onMessageReceived)
    private lateinit var serverScope: CoroutineScope

    private val connections: CoroutineMap<Int, TcpConnection> = CoroutineMap()
    private val connectAtomics: Map<Int, AtomicBoolean>

    private var isCanceled = false

    companion object {
        private const val CONNECT_LOOP_DELAY = 300L
        private val CHECK_CONNECTION_TIMEOUT = (1000L).milliseconds
    }

    init {
        val map: MutableMap<Int, AtomicBoolean> = mutableMapOf()
        others.forEach { node ->
            map[node.id] = AtomicBoolean(false)
        }
        connectAtomics = map
    }

    suspend fun start() {
        serverScope = CoroutineScope(coroutineContext + Dispatchers.IO)
        serverScope.launch {
            server.acceptingLoop()
        }
        connectLoop()
    }

    fun stop() {
        isCanceled = true
        server.stop()
        serverScope.cancel()
    }

    private suspend fun connectLoop() {
        while (!isCanceled) {
            for (node in others) {
                val connection = connections.get(node.id)

                if ((connection == null || connection.isClosed) && shouldConnect(node)) {
                    val connectingAtomic = connectAtomics[node.id]

                    if (!connectingAtomic!!.getAndSet(true))
                        connect(node, connectingAtomic)
                }
                else if (connection != null && isConnectionTimeExpired(connection)) {
                    checkConnection(connection)
                }
            }

            delay(CONNECT_LOOP_DELAY)
        }
    }

    private fun isConnectionTimeExpired(connection: TcpConnection): Boolean {
        return TimeSource.Monotonic.markNow() - connection.lastConnectionTime > CHECK_CONNECTION_TIMEOUT
    }

    private fun shouldConnect(node: Node): Boolean {
        return if (self.id < node.id) false
        else if (self.id > node.id) true
        else throw NodesRepeatIdException("Nodes ids must be unique, self id: ${self.id}, node id: ${node.id}")
    }

    private suspend fun connect(node: Node, atomic: AtomicBoolean) {
        try {
            val socket = aSocket(SelectorManager(Dispatchers.IO)).tcp().connect(node.hostname, node.port)
            val writeChannel = socket.openWriteChannel(autoFlush = true)

            writeChannel.writeInt(self.id)

            val connection = TcpConnection(self, node, socket, onMessageReceived, writeChannel = writeChannel)
            onTcpLooperNodeConnected(connection)

            println("Connected to ${node.id}") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        }
        catch (e: Exception) {
            //System.err.println("Node: ${self.id}. Can not connect to node: ${node.id}")
        }
        finally {
            atomic.set(false)
        }
    }

    private suspend fun onTcpLooperNodeConnected(connection: TcpConnection) {
        val scope = CoroutineScope(coroutineContext + Dispatchers.IO)
        scope.launch {
            connection.readLoop()
        }

        connections.put(connection.other.id, connection)
        onNodeConnected(connection)
    }
}