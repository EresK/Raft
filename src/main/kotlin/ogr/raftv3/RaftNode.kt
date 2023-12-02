package ogr.raftv3

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.*
import ogr.rpc.*
import ogr.transport.Node
import ogr.transport.TcpConnection
import ogr.transport.TcpLooper
import ogr.util.LogEntry
import ogr.util.MessageEntry
import ogr.util.MessageQueue
import ogr.util.MessageWriter
import ogr.util.collection.CoroutineMap
import kotlin.coroutines.coroutineContext
import kotlin.math.min
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class RaftNode(val self: Node, val others: List<Node>) {
    private val looper = TcpLooper(self, others, this::onNodeConnected, this::onMessageReceived, this::checkConnection)
    private val connections: CoroutineMap<Int, TcpConnection> = CoroutineMap()
    private val messageWriter = MessageWriter()
    private val messageQueue = MessageQueue()
    private lateinit var raftServerScope: CoroutineScope

    private val jacksonMapper = jacksonObjectMapper()

    // Server info
    private val state = State(self)
    private val majority: Int = (others.size + 1) / 2 + 1
    private var lastTime = TimeSource.Monotonic.markNow()
    private var isCanceled = false
    private val nodeVotes: MutableMap<Int, Boolean> = mutableMapOf()

    // Volatile state on leader
    private var nextIndex: MutableMap<Int, Int> = mutableMapOf()
    private var matchIndex: MutableMap<Int, Int> = mutableMapOf()

    companion object {
        private val ELECTION_TIMEOUT = (1200..2400L)
        private const val FOLLOWER_LOOP_DELAY = 35L

        private const val CANDIDATE_LOOP_DELAY = 35L
        private const val REQUEST_VOTE_DELAY = 300L
        private const val SEND_REQUEST_VOTE_TIMES = 3

        private const val LEADER_LOOP_DELAY = 35L
        private const val APPEND_ENTRIES_DELAY = 300L

        private const val RAFT_LOOP_DELAY = 0L // 30L
    }

    suspend fun start() {
        println("Majority: $majority") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        raftServerScope = CoroutineScope(coroutineContext + Dispatchers.Default)
        raftServerScope.launch(Dispatchers.IO) { looper.start() }
        raftServerScope.launch { messageWriter.writeLoop() }
        raftLoop()
    }

    fun stop() {
        isCanceled = true
        looper.stop()
        messageWriter.stop()
        raftServerScope.cancel()
    }

    suspend fun appendCommand(command: Int): Boolean {
        val index = state.applyCommand(LogEntry(command, state.currentTerm))

        while (index > state.log.commitIndex) {
            delay(100L)
        }

        return true
    }

    fun isLeader(): Boolean = state.isLeader()

    fun state(): List<LogEntry> {
        println("Commit index: ${state.log.commitIndex}") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        return state.log.entries.take(state.log.commitIndex + 1)
//        return state.log.entries
    }

    private suspend fun raftLoop() {
        while (!isCanceled) {
            println("${state.status}: ${state.currentTerm}") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            when (state.status) {
                RaftStatus.FOLLOWER -> onFollowerStatus()
                RaftStatus.CANDIDATE -> onCandidateStatus()
                RaftStatus.LEADER -> onLeaderStatus()
            }
            delay(RAFT_LOOP_DELAY)
        }
    }

    private suspend fun onFollowerStatus() {
        state.becomeFollower()

        var timeout = nextTimeout()
        updateLastTime()

        loop@ while (timeIsNotExpired(timeout)) {
            val receivedMessage = messageQueue.poll()

            if (receivedMessage == null) {
                delay(FOLLOWER_LOOP_DELAY)
                continue@loop
            }

            val connection = receivedMessage.connection
            val message = receivedMessage.message
            val rpcType = receivedMessage.rpcType

            when (rpcType) {
                RpcType.AppendEntries -> {
                    val appendEntries = jacksonMapper.readValue<AppendEntries>(message)
                    val response = state.appendEntries(appendEntries)

                    if (response.success) {
                        timeout = nextTimeout()
                        updateLastTime()
                    }

                    sendResponse(connection, response)
                }

                RpcType.RequestVote -> {
                    val requestVote = jacksonMapper.readValue<RequestVote>(message)
                    val response = state.requestVote(requestVote)

                    if (response.voteGranted) {
                        timeout = nextTimeout()
                        updateLastTime()
                    }

                    sendResponse(connection, response)
                }

                else -> {}
            }

            delay(FOLLOWER_LOOP_DELAY)
        }

        state.prepareToCandidate()
    }

    private suspend fun onCandidateStatus() {
        state.becomeCandidate()
        resetNodeVotes()

        val timeout = nextTimeout()
        updateLastTime()

        // start sending RequestVote RPCs
        val requestVoteJob = CoroutineScope(coroutineContext + Dispatchers.IO).launch {
            val messages: MutableList<MessageEntry> = mutableListOf()

            val requestVote = RequestVote(
                term = state.currentTerm,
                candidateId = state.self.id,
                lastLogIndex = state.log.lastIndex(),
                lastLogTerm = state.log.lastTerm() ?: 0
            )
            val requestVoteMessage = jacksonMapper.writeValueAsString(requestVote)

            repeat(SEND_REQUEST_VOTE_TIMES) {
                messages.clear()

                if (state.status == RaftStatus.CANDIDATE) {
                    val currentConnections = connections.getValues()

                    for (conn in currentConnections)
                        messages.add(MessageEntry(conn, requestVoteMessage))

                    messageWriter.addAll(messages)
                }

                delay(REQUEST_VOTE_DELAY)
            }
        }

        loop@ while (timeIsNotExpired(timeout)) {
            val receivedMessage = messageQueue.poll()

            if (receivedMessage == null) {
                delay(CANDIDATE_LOOP_DELAY)
                continue@loop
            }

            val connection = receivedMessage.connection
            val message = receivedMessage.message
            val rpcType = receivedMessage.rpcType

            when (rpcType) {
                RpcType.AppendEntries -> {
                    val appendEntries = jacksonMapper.readValue<AppendEntries>(message)
                    val response = state.appendEntries(appendEntries)

                    if (response.success) {
                        state.becomeFollower()
                        break@loop
                    }

                    sendResponse(connection, response)
                }

                RpcType.RequestVote -> {
                    val requestVote = jacksonMapper.readValue<RequestVote>(message)
                    val response = state.requestVote(requestVote)

                    if (response.voteGranted) {
                        state.becomeFollower()
                        break@loop
                    }

                    sendResponse(connection, response)
                }

                RpcType.RequestVoteResponse -> {
                    val response = jacksonMapper.readValue<RequestVoteResponse>(message)

                    if (response.term == state.currentTerm && response.voteGranted) {
                        val firstVote = !nodeVotes[connection.other.id]!!

                        if (firstVote) {
                            nodeVotes[connection.other.id] = true

                            val votes = state.grantedVotes.incrementAndGet()

                            if (votes >= majority)
                                state.becomeLeader()
                        }
                    }
                }

                else -> {}
            }

            delay(CANDIDATE_LOOP_DELAY)
        }

        // stop sending RequestVote RPCs
        requestVoteJob.cancel()
    }

    private suspend fun onLeaderStatus() {
        state.becomeLeader()

        // for each server update nextIndex && matchIndex
        updateNextAndMatchIndices()

        // start sending AppendEntries RPCs
        val appendEntriesJob = CoroutineScope(coroutineContext + Dispatchers.IO).launch {
            val messages: MutableList<MessageEntry> = mutableListOf()

            // TODO: OPTIMIZE MEMORY REUSE
            while (!isCanceled && state.status == RaftStatus.LEADER) {
                val currentConnections = connections.getValues()
                messages.clear()

                for (conn in currentConnections) {
                    val prevIndex = nextIndex[conn.other.id]!! - 1
                    val prevTerm = if (prevIndex > -1) state.log[prevIndex]?.term!! else -1
                    val entries = state.log.lastStartsAt(prevIndex + 1)

                    val appendEntries = AppendEntries(
                        term = state.currentTerm,
                        leaderId = state.self.id,
                        prevLogIndex = prevIndex,
                        prevLogTerm = prevTerm,
                        entries = entries,
                        leaderCommit = state.log.commitIndex
                    )
                    val appendEntriesMessage = jacksonMapper.writeValueAsString(appendEntries)

                    messages.add(MessageEntry(conn, appendEntriesMessage))
                }

                messageWriter.addAll(messages)

                delay(APPEND_ENTRIES_DELAY)
            }
        }

        loop@ while (!isCanceled) {
            val receivedMessage = messageQueue.poll()

            if (receivedMessage == null) {
                delay(LEADER_LOOP_DELAY)
                continue@loop
            }

            val connection = receivedMessage.connection
            val message = receivedMessage.message
            val rpcType = receivedMessage.rpcType

            when (rpcType) {
                RpcType.AppendEntries -> {
                    val appendEntries = jacksonMapper.readValue<AppendEntries>(message)
                    val response = state.appendEntries(appendEntries)

                    if (response.success) {
                        state.becomeFollower()
                        break@loop
                    }

                    sendResponse(connection, response)
                }

                RpcType.RequestVote -> {
                    val requestVote = jacksonMapper.readValue<RequestVote>(message)
                    val response = state.requestVote(requestVote)

                    if (response.voteGranted) {
                        state.becomeFollower()
                        break@loop
                    }

                    sendResponse(connection, response)
                }

                RpcType.AppendEntriesResponse -> {
                    val response = jacksonMapper.readValue<AppendEntriesResponse>(message)

                    // Success
                    if (response.term == state.currentTerm && response.success) {
                        val nodeNextIndex = nextIndex[connection.other.id]!!
                        val nodeMatchIndex = matchIndex[connection.other.id]!!

                        if (nodeNextIndex < state.log.lastIndex()) {
                            nextIndex[connection.other.id] = nodeNextIndex + 1
                        }

                        if (nodeMatchIndex < state.log.lastIndex()) {
                            matchIndex[connection.other.id] = nodeMatchIndex + 1
                        }

                        // Check that log is committed
                        if (isLogAtIndexReplicated(nodeNextIndex)) {
                            state.log.commit(nodeNextIndex)
                        }
                    }
                    // Fail
                    else if (response.term == state.currentTerm) {
                        nextIndex[connection.other.id] = min(0, nextIndex[connection.other.id]!! - 1)
                    }

                    if (response.term > state.currentTerm) {
                        state.becomeFollower()
                        break@loop
                    }
                }

                else -> {}
            }

            delay(LEADER_LOOP_DELAY)
        }

        // stop sending AppendEntries RPCs
        appendEntriesJob.cancel()
    }

    /* Supportive functions */
    private fun isLogAtIndexReplicated(index: Int): Boolean {
        val count = matchIndex.values.count { it >= index } + 1
        return count >= majority
    }

    private fun updateNextAndMatchIndices() {
        for (node in others) {
            nextIndex[node.id] = 0
            matchIndex[node.id] = 0
        }
    }

    private fun resetNodeVotes() {
        for (node in others)
            nodeVotes[node.id] = false
    }

    private suspend fun sendResponse(connection: TcpConnection, response: Any) {
        val message = jacksonMapper.writeValueAsString(response)
        messageWriter.add(connection, message)
    }

    private fun updateLastTime() {
        lastTime = TimeSource.Monotonic.markNow()
    }

    private fun nextTimeout(): Long {
        return ELECTION_TIMEOUT.random()
    }

    private fun timeIsNotExpired(timeout: Long): Boolean {
        return TimeSource.Monotonic.markNow() - lastTime < timeout.milliseconds
    }

    /* Callback functions */
    private suspend fun onNodeConnected(connection: TcpConnection) {
        connections.put(connection.other.id, connection)
    }

    private suspend fun onMessageReceived(connection: TcpConnection, message: String) {
        messageQueue.add(connection, message)
    }

    private suspend fun checkConnection(connection: TcpConnection) {
        val checkConnection = CheckConnection()
        val checkConnectionMessage = jacksonMapper.writeValueAsString(checkConnection)
        messageWriter.add(connection, checkConnectionMessage)
    }
}