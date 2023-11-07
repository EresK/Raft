package algorithm.transport

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import tcp.NodesRepeatIdException
import java.util.concurrent.atomic.AtomicBoolean

class TcpLooper(
    val self: Node,
    val others: List<Node>,
    private val onNodeConnected: suspend (TcpConnection) -> Unit,
    private val onMessageReceived: suspend (TcpConnection, String) -> Unit
) {
    @Volatile
    var isCanceled = false
        private set

    private val tcpServer: TcpServer = TcpServer(self, others, this::onLooperNodeConnected, onMessageReceived)

    private val tcpConnections: MutableMap<Int, TcpConnection> = mutableMapOf()
    private val tcpMutex = Mutex()

    private val connectingAtomics: Map<Int, AtomicBoolean>

    private lateinit var looperScope: CoroutineScope

    companion object {
        private const val CONNECTING_LOOP_DELAY = 30L
    }

    init {
        val map: MutableMap<Int, AtomicBoolean> = mutableMapOf()
        others.forEach { node -> map[node.id] = AtomicBoolean(false) }
        connectingAtomics = map
    }

    fun start(scope: CoroutineScope) {
        looperScope = CoroutineScope(scope.coroutineContext + Dispatchers.IO + CoroutineName("TcpLooper"))

        tcpServer.launchAcceptingLoop(looperScope)
        launchConnectingLoop(looperScope)
    }

    fun stop() {
        isCanceled = true
    }

    private fun launchConnectingLoop(scope: CoroutineScope) {
        scope.launch {
            println("Connecting loop $coroutineContext") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            while (!isCanceled) {
                for (node in others) {
                    val connection = getConnection(node)

                    if ((connection == null || connection.isClosed) && shouldConnect(node)) {
                        val connecting = connectingAtomics[node.id]

                        if (!connecting!!.getAndSet(true)) {
                            connect(node, connecting, this)
                        }
                    }
                    // else if check connection
                }

                delay(CONNECTING_LOOP_DELAY)
            }
            cancel() // cancel all other connections
        }
    }

    private suspend fun getConnection(node: Node): TcpConnection? {
        return tcpMutex.withLock {
            tcpConnections[node.id]
        }
    }

    private fun shouldConnect(node: Node): Boolean {
        return if (self.id < node.id) false
        else if (self.id > node.id) true
        else throw NodesRepeatIdException("Nodes ids must be unique, self id: ${self.id}, node id: ${node.id}")
    }

    private suspend fun connect(node: Node, atomic: AtomicBoolean, scope: CoroutineScope) {
        scope.launch {
            println("Connect $coroutineContext") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            try {
                val socket = aSocket(SelectorManager(Dispatchers.IO)).tcp().connect(node.hostname, node.port)
                val writeChannel = socket.openWriteChannel(autoFlush = true)

                writeChannel.writeInt(self.id)

                val connection = TcpConnection(self, node, socket, onMessageReceived, writeChannel = writeChannel)
                onLooperNodeConnected(connection)

                println("Connected to node ${node.id}") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            }
            catch (e: Exception) {
                System.err.println("Node: ${self.id}. Can not connect to node: ${node.id}")
            }
            finally {
                atomic.set(false)
            }
            cancel()
        }
    }

    private suspend fun onLooperNodeConnected(connection: TcpConnection) {
        tcpMutex.withLock {
            val key = connection.other.id
            tcpConnections.put(key, connection)
        }

        connection.launchReadingLoop(looperScope)
        onNodeConnected(connection)
    }
}