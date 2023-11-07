package algorithm.raft

import algorithm.raft.rpc.*
import algorithm.transport.Node
import algorithm.transport.TcpConnection
import algorithm.transport.TcpLooper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class RaftServer(val self: Node, val others: List<Node>) {
    private val tcpLooper = TcpLooper(self, others, this::onNodeConnected, this::onMessageReceived)
    private lateinit var serverScope: CoroutineScope

    private val tcpConnections: MutableMap<Int, TcpConnection> = mutableMapOf()
    private val tcpMutex = Mutex()

    private val jacksonMapper = jacksonObjectMapper()

    private val statusDelegate: Stack<suspend () -> Unit> = Stack()

    @Volatile
    var isCanceled = false
        private set

    // Server info
    private var status: ServerStatus = ServerStatus.FOLLOWER
    private var localTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()
    private var cancelled = false

    // Persistent state
    private var currentTerm = 0
    private var votedFor: Int? = null
    private val log: List<String> = emptyList()

    // Volatile state
    private var commitIndex = 0
    private var lastApplied = 0

    companion object {
        private val ELECTION_TIMEOUT = (300L..450L)
        private const val FOLLOWER_LOOP_DELAY = 30L
        private const val CANDIDATE_LOOP_DELAY = 30L
        private const val LEADER_LOOP_DELAY = 30L
    }

    init {
        statusDelegate.push(this::onFollowerStatus)
    }

    fun start(scope: CoroutineScope) {
        serverScope = CoroutineScope(scope.coroutineContext + CoroutineName("RaftServer"))
        tcpLooper.start(serverScope)
        launchServerLoop(serverScope)
    }

    fun stop() {
        isCanceled = true
        tcpLooper.stop()
        serverScope.cancel()
    }

    private fun launchServerLoop(scope: CoroutineScope) {
        /*
        scope.launch {
            println("Server loop $coroutineContext")

            delay(3_000)

            repeat(3) {
                tcpMutex.withLock {
                    for (conn in tcpConnections.values) {
                        conn.write("Hello from node ${self.id}")
                    }
                }
                delay(1_000)
            }
        }
        */

        scope.launch {
            while (!cancelled && !statusDelegate.empty()) {
                val delegate = statusDelegate.pop()
                delegate.invoke()
            }
        }
    }

    private suspend fun onFollowerStatus() {
        println("onFollowerStatus") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        status = ServerStatus.FOLLOWER

        val timeout = ELECTION_TIMEOUT.random().milliseconds

        while (TimeSource.Monotonic.markNow() - localTime < timeout) {
            delay(FOLLOWER_LOOP_DELAY)
        }

        statusDelegate.push(this::onCandidateStatus)
    }

    private suspend fun onCandidateStatus() {
        println("onCandidateStatus") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        status = ServerStatus.CANDIDATE
        currentTerm += 1
        votedFor = self.id

        val timeout = ELECTION_TIMEOUT.random().milliseconds

        val requestVote = RequestVote(
            term = currentTerm,
            candidateId = self.id,
            lastLogIndex = 0,
            lastLogTerm = 0
        )
        val requestVoteMessage = jacksonMapper.writeValueAsString(requestVote)

        val connections = getAllConnections()
        val scope = CoroutineScope(coroutineContext + Dispatchers.IO)

        connections.forEach { conn ->
            scope.launch(Dispatchers.IO) {
                conn.write(requestVoteMessage)
                cancel()
            }
        }

        while (TimeSource.Monotonic.markNow() - localTime < timeout) {
            // if granted votes >= majority then become leader
            // statusChangeDelegate = this::onLeaderStatus

            delay(CANDIDATE_LOOP_DELAY)
        }

        if (statusDelegate.empty())
            statusDelegate.push(this::onFollowerStatus)
    }

    private suspend fun onLeaderStatus() {
        println("onLeaderStatus") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        while (!isCanceled) {
            val appendEntries = AppendEntries(
                term = currentTerm,
                leaderId = self.id,
                prevLogIndex = 0,
                prevLogTerm = 0,
                entries = listOf(),
                leaderCommit = 0,
            )
            val appendEntriesMessage = jacksonMapper.writeValueAsString(appendEntries)

            val connections = getAllConnections()
            val scope = CoroutineScope(coroutineContext + Dispatchers.IO)

            connections.forEach { conn ->
                scope.launch(Dispatchers.IO) {
                    conn.write(appendEntriesMessage)
                    cancel()
                }
            }

            delay(LEADER_LOOP_DELAY)
        }
    }

    private suspend fun onNodeConnected(connection: TcpConnection) {
        tcpMutex.withLock {
            val key = connection.other.id
            tcpConnections.put(key, connection)
        }
    }

    private suspend fun onMessageReceived(connection: TcpConnection, message: String) {
        println("Message from node ${connection.other.id}: $message") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        var rpcType: RpcType? = null

        try {
            val messageTypeString = jacksonMapper.readTree(message).get("type")
            rpcType = jacksonMapper.treeToValue(messageTypeString, RpcType::class.java)
        }
        catch (e: Exception) {
            System.err.println("Can not recognize type of received message: $message")
        }

        when (rpcType) {
            RpcType.AppendEntries -> {
                val appendEntries = jacksonMapper.readValue<AppendEntries>(message)
                onAppendEntriesReceived(appendEntries)
            }

            RpcType.AppendEntriesResp -> {
                val response = jacksonMapper.readValue<AppendEntriesResp>(message)
                onAppendEntriesResponseReceived(response)
            }

            RpcType.RequestVote -> {
                val requestVote = jacksonMapper.readValue<RequestVote>(message)
                onRequestVoteReceived(requestVote)
            }

            RpcType.RequestVoteResp -> {
                val response = jacksonMapper.readValue<RequestVoteResp>(message)
                onRequestVoteResponseReceived(response)
            }

            else -> {}
        }
    }

    private fun onAppendEntriesReceived(appendEntries: AppendEntries) {
        if (status == ServerStatus.FOLLOWER)
            localTime = TimeSource.Monotonic.markNow()
        else if (status == ServerStatus.CANDIDATE && appendEntries.term >= currentTerm) {
            localTime = TimeSource.Monotonic.markNow()

            // TODO: exclusion or callback
            status = ServerStatus.FOLLOWER
        }
    }

    private fun onAppendEntriesResponseReceived(response: AppendEntriesResp) {
    }

    private fun onRequestVoteReceived(requestVote: RequestVote) {
    }

    private fun onRequestVoteResponseReceived(response: RequestVoteResp) {
    }

    private suspend fun getConnection(node: Node): TcpConnection? {
        return tcpMutex.withLock {
            tcpConnections[node.id]
        }
    }

    private suspend fun getAllConnections(): List<TcpConnection> {
        return tcpMutex.withLock {
            tcpConnections.values.toList()
        }
    }
}