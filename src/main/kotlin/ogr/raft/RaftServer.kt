package ogr.raft

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.*
import ogr.rpc.*
import ogr.transport.Node
import ogr.transport.TcpConnection
import ogr.transport.TcpLooper
import ogr.util.MessageEntry
import ogr.util.MessageQueue
import ogr.util.MessageWriter
import ogr.util.collection.CoroutineMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.math.min
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class RaftServer(val self: Node, val others: List<Node>) {
    private val looper = TcpLooper(self, others, this::onNodeConnected, this::onMessageReceived, this::checkConnection)
    private val connections: CoroutineMap<Int, TcpConnection> = CoroutineMap()
    private val messageWriter = MessageWriter()
    private val messageQueue = MessageQueue()
    private lateinit var raftServerScope: CoroutineScope

    private val jacksonMapper = jacksonObjectMapper()

    // Server info
    private var status = RaftStatus.FOLLOWER
    private val majority: Int = (others.size + 2) / 2
    private var lastTime = TimeSource.Monotonic.markNow()
    private val grantedVotes: AtomicInteger = AtomicInteger(0)
    private var isCanceled = false

    // Persistent state
    private var currentTerm = 0
    private var votedFor: Int? = null
    private val log: MutableList<LogEntry> = mutableListOf()

    // Volatile state
    private var commitIndex = 0
    private var lastApplied = 0

    // Volatile state on leader
    private var nextIndex: Map<Int, Int> = mapOf()
    private var matchIndex: Map<Int, Int> = mapOf()

    companion object {
        private val ELECTION_TIMEOUT = (600..1200L)
        private const val FOLLOWER_LOOP_DELAY = 30L
        private const val CANDIDATE_LOOP_DELAY = 30L
        private const val LEADER_LOOP_DELAY = 30L
        private const val RAFT_LOOP_DELAY = 30L
    }

    suspend fun start() {
        raftServerScope = CoroutineScope(coroutineContext + Dispatchers.Default)

        raftServerScope.launch(Dispatchers.IO) {
            looper.start()
        }

        raftServerScope.launch {
            messageWriter.writeLoop()
        }

        raftLoop()
    }

    fun stop() {
        isCanceled = true
        looper.stop()
        messageWriter.stop()
        raftServerScope.cancel()
    }

    private suspend fun raftLoop() {
        while (!isCanceled) {
            println(status) // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            when (status) {
                RaftStatus.FOLLOWER -> onFollowerStatus()
                RaftStatus.CANDIDATE -> onCandidateStatus()
                RaftStatus.LEADER -> onLeaderStatus()
            }

            delay(RAFT_LOOP_DELAY)
        }
    }

    private suspend fun onFollowerStatus() {
        val timeout = ELECTION_TIMEOUT.random()
        updateLastTime()

        loop@ while (!isElectionTimeExpired(timeout)) {
            val receivedMessage = messageQueue.poll()

            if (receivedMessage == null) {
                delay(FOLLOWER_LOOP_DELAY)
                continue@loop
            }

            val connection = receivedMessage.connection
            val message = receivedMessage.message
            val rpcType = receivedMessage.rpcType

            when (rpcType) {
                RpcType.AppendEntries -> onAppendEntries(connection, message)

                RpcType.RequestVote -> onRequestVote(connection, message)

                else -> {}
            }

            delay(FOLLOWER_LOOP_DELAY)
        }

        status = RaftStatus.CANDIDATE
    }

    private suspend fun onCandidateStatus() {
        currentTerm += 1
        votedFor = self.id
        grantedVotes.set(1)

        val timeout = ELECTION_TIMEOUT.random()
        updateLastTime()

        loop@ while (!isElectionTimeExpired(timeout)) {
            sendRequestVoteRpc()

            // Read messages
            val receivedMessage = messageQueue.poll()

            if (receivedMessage == null) {
                delay(FOLLOWER_LOOP_DELAY)
                continue@loop
            }

            val connection = receivedMessage.connection
            val message = receivedMessage.message
            val rpcType = receivedMessage.rpcType

            // Process RPC
            when (rpcType) {
                RpcType.AppendEntries -> onAppendEntries(connection, message)

                RpcType.RequestVote -> onRequestVote(connection, message)

                RpcType.RequestVoteResponse -> onRequestVoteResponse(message)

                else -> {}
            }

            if (status != RaftStatus.CANDIDATE)
                break@loop

            delay(CANDIDATE_LOOP_DELAY)
        }
    }

    private suspend fun sendRequestVoteRpc() {
        val requestVote = RequestVote(
            term = currentTerm,
            candidateId = self.id,
            lastLogIndex = if (log.isEmpty()) 0 else log.size - 1,
            lastLogTerm = if (log.isEmpty()) 0 else log.last().term
        )
        val requestVoteMessage = jacksonMapper.writeValueAsString(requestVote)
        val currentConnections = connections.getValues()

        val messages = currentConnections.map { conn ->
            MessageEntry(conn, requestVoteMessage)
        }.toList()

        messageWriter.addAll(messages)
    }

    private suspend fun onAppendEntries(connection: TcpConnection, message: String) {
        val appendEntries = jacksonMapper.readValue<AppendEntries>(message)

        fun containsEntry(appendEntries: AppendEntries): Boolean {
            return log.isEmpty() || (appendEntries.prevLogIndex < log.size &&
                    appendEntries.prevLogTerm == log[appendEntries.prevLogIndex].term)
        }

        fun conflictEntry(appendEntries: AppendEntries): Boolean {
            return log.isNotEmpty() &&
                    appendEntries.prevLogIndex < log.size &&
                    appendEntries.prevLogTerm != log[appendEntries.prevLogIndex].term
        }

        when (status) {
            RaftStatus.FOLLOWER, RaftStatus.CANDIDATE -> {
                var success = true

                // Step 1 & 2
                if (appendEntries.term < currentTerm || !containsEntry(appendEntries)) {
                    success = false
                }

                if (success) {
                    // Candidate become follower, discover current leader
                    status = RaftStatus.FOLLOWER
                    // Update term
                    currentTerm = appendEntries.term

                    // Step 3
                    if (conflictEntry(appendEntries)) {
                        repeat(log.size - appendEntries.prevLogIndex) {
                            log.removeLast()
                        }
                    }

                    // Step 4
                    log.addAll(appendEntries.entries)

                    // Step 5
                    if (commitIndex < appendEntries.leaderCommit) {
                        val indexLastNewEntry =
                            if (appendEntries.entries.isNotEmpty()) (log.size - 1) else Int.MAX_VALUE
                        commitIndex = min(appendEntries.leaderCommit, indexLastNewEntry)
                    }
                }

                // Response
                val response = AppendEntriesResponse(currentTerm, success)
                val responseMessage = jacksonMapper.writeValueAsString(response)
                messageWriter.add(connection, responseMessage)

                if (success)
                    updateLastTime()
            }

            RaftStatus.LEADER -> {
                // TODO
            }
        }
    }

    private suspend fun onRequestVote(connection: TcpConnection, message: String) {
        val requestVote = jacksonMapper.readValue<RequestVote>(message)

        when (status) {
            RaftStatus.FOLLOWER -> {
                var grantVote = false

                // Step 1 & 2
                if (requestVote.term >= currentTerm &&
                    (votedFor == null || votedFor == requestVote.candidateId) &&
                    isLogUpToDate(requestVote)) {
                    // Grant vote && update term
                    grantVote = true
                    currentTerm = requestVote.term
                }

                val response = RequestVoteResponse(currentTerm, grantVote)
                val responseMessage = jacksonMapper.writeValueAsString(response)
                messageWriter.add(connection, responseMessage)

                if (grantVote)
                    updateLastTime()
            }

            RaftStatus.CANDIDATE -> {
                // Discovers new term
                // Become follower && update term && change voted for
                if (requestVote.term > currentTerm &&
                    isLogUpToDate(requestVote)) {
                    status = RaftStatus.FOLLOWER
                    currentTerm = requestVote.term
                    votedFor = requestVote.candidateId

                    val response = RequestVoteResponse(currentTerm, true)
                    val responseMessage = jacksonMapper.writeValueAsString(response)
                    messageWriter.add(connection, responseMessage)
                }
            }

            RaftStatus.LEADER -> {
                // TODO
            }
        }
    }

    private fun onRequestVoteResponse(message: String) {
        val response = jacksonMapper.readValue<RequestVoteResponse>(message)

        if (status == RaftStatus.CANDIDATE &&
            response.term == currentTerm &&
            response.voteGranted) {
            val votes = grantedVotes.incrementAndGet()

            if (votes >= majority)
                status = RaftStatus.LEADER
        }
    }

    private suspend fun onLeaderStatus() {
        loop@ while (true) {
            val currentConnections = connections.getValues()

            for (conn in currentConnections) {
                val appendEntries = AppendEntries(
                    term = currentTerm,
                    leaderId = self.id,
                    prevLogIndex = 0, // nextIndex[conn.other.id]!!,
                    prevLogTerm = 0, //matchIndex[conn.other.id]!!,
                    entries = log,
                    leaderCommit = commitIndex
                )
                val appendEntriesMessage = jacksonMapper.writeValueAsString(appendEntries)

                messageWriter.add(conn, appendEntriesMessage)
            }

            // Read messages
//            val receivedMessage = messageQueue.poll()
//
//            if (receivedMessage == null) {
//                delay(FOLLOWER_LOOP_DELAY)
//                continue@loop
//            }
//
//            val connection = receivedMessage.connection
//            val message = receivedMessage.message
//
//            when (receivedMessage.rpcType) {
//                RpcType.AppendEntries -> {}
//                RpcType.AppendEntriesResponse -> {}
//                RpcType.RequestVote -> {}
//                else -> {}
//            }

            delay(LEADER_LOOP_DELAY)
        }
    }

    /* Supportive functions */
    private fun isElectionTimeExpired(timeout: Long): Boolean {
        return TimeSource.Monotonic.markNow() - lastTime > timeout.milliseconds
    }

    private fun updateLastTime() {
        lastTime = TimeSource.Monotonic.markNow()
    }

    private fun isLogUpToDate(requestVote: RequestVote): Boolean { // TODO: check this function's correctness
        return if (log.isEmpty())
            true
        else if (requestVote.lastLogTerm < log.last().term)
            false
        else if (requestVote.lastLogTerm > log.last().term)
            true
        else if (requestVote.lastLogIndex + 1 >= log.size)
            true
        else
            false
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