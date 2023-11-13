package ogr.raft

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import ogr.rpc.*
import ogr.transport.Node
import ogr.transport.TcpConnection
import ogr.transport.TcpLooper
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
    private val status = RaftStatus.FOLLOWER
    private val majority: Int = (others.size + 2) / 2
    private var lastTime = TimeSource.Monotonic.markNow()
    private val grantedVotes: AtomicInteger = AtomicInteger(0)

    // Persistent state
    private var currentTerm = 0
    private var votedFor: Int? = null
    private val log: MutableList<LogEntry> = mutableListOf()

    // Volatile state
    private var commitIndex = 0
    private var lastApplied = 0

    // Volatile state on leader
    private var nextIndex: List<Int> = listOf()
    private var matchIndex: List<Int> = listOf()

    companion object {
        private val FOLLOWER_TIMEOUT = (500..1000L)
        private const val FOLLOWER_LOOP_DELAY = 30L
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

    private suspend fun raftLoop() {
        while (true) {
            when (status) {
                RaftStatus.FOLLOWER -> onFollowerStatus()
                RaftStatus.CANDIDATE -> TODO()
                RaftStatus.LEADER -> TODO()
            }
        }
    }

    private suspend fun onFollowerStatus() {
        val timeout = FOLLOWER_TIMEOUT.random()

        while (!isElectionTimeExpired(timeout)) {
            val receivedMessage = messageQueue.poll()

            if (receivedMessage == null) {
                delay(FOLLOWER_LOOP_DELAY)
                continue
            }

            val connection = receivedMessage.connection
            val message = receivedMessage.message

            when (receivedMessage.rpcType) {
                RpcType.AppendEntries -> {
                    val appendEntries = jacksonMapper.readValue<AppendEntries>(message)

                    fun containsEntry(appendEntries: AppendEntries): Boolean {
                        return log.isNotEmpty() &&
                                appendEntries.prevLogIndex < log.size &&
                                appendEntries.prevLogTerm == log[appendEntries.prevLogIndex].term
                    }

                    fun conflictEntry(appendEntries: AppendEntries): Boolean {
                        return log.isNotEmpty() &&
                                appendEntries.prevLogIndex < log.size &&
                                appendEntries.prevLogTerm != log[appendEntries.prevLogIndex].term
                    }

                    if (appendEntries.term < currentTerm || !containsEntry(appendEntries)) { // 1 & 2
                        val response = AppendEntriesResponse(currentTerm, false)
                        val responseMessage = jacksonMapper.writeValueAsString(response)

                        messageWriter.add(connection, responseMessage)
                        break
                    }
                    else if (conflictEntry(appendEntries)) { // 3
                        while (appendEntries.prevLogIndex < log.size) {
                            log.removeLast()
                        }
                    }

                    log.addAll(appendEntries.entries) // 4

                    if (commitIndex < appendEntries.leaderCommit) { // 5
                        val indexLastNewEntry =
                            if (appendEntries.entries.isNotEmpty()) (log.size - 1) else Int.MAX_VALUE
                        commitIndex = min(appendEntries.leaderCommit, indexLastNewEntry)
                    }

                    updateLastTime()
                }

                RpcType.RequestVote -> {
                    val requestVote = jacksonMapper.readValue<RequestVote>(message)

                    fun isLogUpToDate(requestVote: RequestVote): Boolean { // TODO: check this function's correctness
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

                    var grantVote = false

                    if (requestVote.term < currentTerm) { // 1
                        grantVote = false
                    }
                    else if ((votedFor == null || votedFor == requestVote.candidateId) && isLogUpToDate(requestVote)) { // 2
                        grantVote = true
                    }

                    val response = RequestVoteResponse(currentTerm, grantVote)
                    val responseMessage = jacksonMapper.writeValueAsString(response)

                    messageWriter.add(connection, responseMessage)

                    if (grantVote)
                        updateLastTime()
                }

                else -> {}
            }

            delay(FOLLOWER_LOOP_DELAY)
        }
    }

    private fun onCandidateStatus() {

    }

    private fun isElectionTimeExpired(timeout: Long): Boolean {
        return TimeSource.Monotonic.markNow() - lastTime > timeout.milliseconds
    }

    private fun updateLastTime() {
        lastTime = TimeSource.Monotonic.markNow()
    }

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