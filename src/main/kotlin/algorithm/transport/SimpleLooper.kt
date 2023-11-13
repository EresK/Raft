package algorithm.transport

import algorithm.util.CoroutineMap
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*
import tcp.NodesRepeatIdException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class SimpleLooper(
    val self: Node,
    val others: List<Node>,
    private val onNodeConnected: suspend (SimpleConnection) -> Unit,
    private val onMessageReceived: suspend (SimpleConnection, String) -> Unit,
    private val connectionCheck: suspend (SimpleConnection) -> Unit
) {
    private var isCanceled = false

    private val simpleServer = SimpleServer(self, others, this::onLooperNodeConnected, onMessageReceived)
    private lateinit var simpleServerScope: CoroutineScope
    private val connections: CoroutineMap<Int, SimpleConnection> = CoroutineMap()
    private val connectingAtomics: Map<Int, AtomicBoolean>

    companion object {
        private const val CONNECTING_LOOP_DELAY = 30L
        private val CHECK_CONNECTION_TIMEOUT = (120L).milliseconds
    }

    init {
        val map: MutableMap<Int, AtomicBoolean> = mutableMapOf()
        others.forEach { node -> map[node.id] = AtomicBoolean(false) }
        connectingAtomics = map
    }

    suspend fun start() {
        simpleServerScope = CoroutineScope(coroutineContext + Dispatchers.IO)
        simpleServerScope.launch {
            simpleServer.acceptingLoop()
        }
        connectingLoop()
    }

    fun stop() {
        isCanceled = true
        simpleServer.stop()
        simpleServerScope.cancel()
    }

    private suspend fun connectingLoop() {
        while (!isCanceled) {
            for (node in others) {
                val connection = connections.get(node.id)

                // TODO: Check max connections try, implement retry mechanism
                if ((connection == null || connection.isClosed) && shouldConnect(node)) {
                    val connectingAtomic = connectingAtomics[node.id]

                    if (!connectingAtomic!!.getAndSet(true))
                        connect(node, connectingAtomic)
                }
                else if (connection != null &&
                    (TimeSource.Monotonic.markNow() - connection.lastConnectionTime > CHECK_CONNECTION_TIMEOUT)) {
                    connectionCheck(connection)
                }
            }

            delay(CONNECTING_LOOP_DELAY)
        }
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

            val connection = SimpleConnection(self, node, socket, onMessageReceived, writeChannel = writeChannel)
            onLooperNodeConnected(connection)

            println("Connected to ${node.id}") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        }
        catch (e: Exception) {
            System.err.println("Node: ${self.id}. Can not connect to node: ${node.id}")
        }
        finally {
            atomic.set(false)
        }
    }

    private suspend fun onLooperNodeConnected(connection: SimpleConnection) {
        val scope = CoroutineScope(coroutineContext + Dispatchers.IO)
        scope.launch {
            connection.readingLoop()
        }

        connections.put(connection.other.id, connection)
        onNodeConnected(connection)
    }
}