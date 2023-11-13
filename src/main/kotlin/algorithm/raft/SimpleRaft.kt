package algorithm.raft

import algorithm.raft.entry.LogEntry
import algorithm.raft.entry.WriterEntry
import algorithm.raft.rpc.*
import algorithm.transport.Node
import algorithm.transport.SimpleConnection
import algorithm.transport.SimpleLooper
import algorithm.util.CoroutineMap
import algorithm.util.CoroutineQueue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.*
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.math.ceil
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class SimpleRaft(val self: Node, val others: List<Node>) {
    private val simpleLooper =
        SimpleLooper(self, others, this::onNodeConnected, this::onMessageReceived, this::connectionCheck)
    private val connections: CoroutineMap<Int, SimpleConnection> = CoroutineMap()
    private val jacksonMapper = jacksonObjectMapper()

    private val statusChangedCallbacks: Stack<suspend () -> Unit> = Stack()
    private val writerQueue: CoroutineQueue<WriterEntry> = CoroutineQueue()
    private lateinit var raftServerScope: CoroutineScope

    // Server info
    private var status: ServerStatus = ServerStatus.FOLLOWER
    private var localTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()
    private val grantedVotes: AtomicInteger = AtomicInteger(0)
    private val majority: Int
    private var isCanceled = false

    // Persistent state
    private var currentTerm = 0
    private var votedFor: Int? = null
    private val log: MutableList<LogEntry> = mutableListOf()

    // Volatile state
    private var commitIndex = 0
    private var lastApplied = 0

    companion object {
        // broadcastTime << electionTimeout << MTBF
        private val ELECTION_TIMEOUT = (600..1200L)
        private const val HEARTBEAT_TIMEOUT = 100L
        private const val FOLLOWER_LOOP_DELAY = 30L
        private const val CANDIDATE_LOOP_DELAY = 30L
        private const val SERVER_LOOP_DELAY = 30L
        private const val WRITER_LOOP_DELAY = 30L
    }

    init {
        statusChangedCallbacks.push(this::onFollowerStatus)
        majority = ceil(others.size / 2.0).toInt() + 1
    }

    suspend fun start() {
        raftServerScope = CoroutineScope(coroutineContext + Dispatchers.Default)

        raftServerScope.launch(Dispatchers.IO) {
            simpleLooper.start()
        }
        raftServerScope.launch {
            serverLoop()
        }
        raftServerScope.launch(Dispatchers.IO) {
            writerLoop()
        }
    }

    fun stop() {
        isCanceled = true
        simpleLooper.stop()
        raftServerScope.cancel()
    }

    private suspend fun serverLoop() {
        while (statusChangedCallbacks.isNotEmpty() && !isCanceled) {
            val callback = statusChangedCallbacks.pop()
            callback.invoke()
            delay(SERVER_LOOP_DELAY)
        }
    }

    private suspend fun writerLoop() {
        while (!isCanceled) {
            val writeTask = writerQueue.poll()

            if (writeTask != null)
                tryWriteMessage(writeTask.connection, writeTask.message)

            delay(WRITER_LOOP_DELAY)
        }
    }

    private suspend fun tryWriteMessage(conn: SimpleConnection, message: String) {
        if (!conn.isClosed)
            conn.write(message)
        else
            connections.remove(conn.other.id)
    }

    private suspend fun onFollowerStatus() {
        println("FOLLOWER") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        status = ServerStatus.FOLLOWER
        votedFor = null

        val timeout = ELECTION_TIMEOUT.random().milliseconds
        localTime = TimeSource.Monotonic.markNow()

        while (TimeSource.Monotonic.markNow() - localTime < timeout && !isCanceled) {
            delay(FOLLOWER_LOOP_DELAY)
        }

        statusChangedCallbacks.push(this::onCandidateStatus) // become candidate
    }

    private suspend fun onCandidateStatus() {
        println("CANDIDATE") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        status = ServerStatus.CANDIDATE
        currentTerm += 1
        votedFor = self.id
        grantedVotes.incrementAndGet()

        val timeout = ELECTION_TIMEOUT.random().milliseconds
        localTime = TimeSource.Monotonic.markNow()

        val requestVote = RequestVote(
            term = currentTerm,
            candidateId = self.id,
            lastLogIndex = 0,
            lastLogTerm = 0
        )
        val requestVoteMessage = jacksonMapper.writeValueAsString(requestVote)

        val currentConnections = connections.getValues()

        withContext(Dispatchers.IO) {
            currentConnections.forEach { conn ->
                writerQueue.add(WriterEntry(conn, requestVoteMessage))
            }
        }

        while (TimeSource.Monotonic.markNow() - localTime < timeout && !isCanceled) {
            if (grantedVotes.get() >= majority)
                statusChangedCallbacks.push(this::onLeaderStatus) // become leader

            delay(CANDIDATE_LOOP_DELAY)
        }

        grantedVotes.set(0) // clear granted votes because election time is out

        if (statusChangedCallbacks.empty())
            statusChangedCallbacks.push(this::onFollowerStatus) // TODO: become follower or candidate?
    }

    private suspend fun onLeaderStatus() {
        println("LEADER") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        status = ServerStatus.LEADER

        while (status == ServerStatus.LEADER && !isCanceled) {
            val appendEntries = AppendEntries(
                term = currentTerm,
                leaderId = self.id,
                prevLogIndex = 0,
                prevLogTerm = 0,
                entries = listOf(),
                leaderCommit = 0
            )
            val appendEntriesMessage = jacksonMapper.writeValueAsString(appendEntries)

            val currentConnections = connections.getValues()

            withContext(Dispatchers.IO) {
                currentConnections.forEach { conn ->
                    writerQueue.add(WriterEntry(conn, appendEntriesMessage))
                }
            }

            delay(HEARTBEAT_TIMEOUT)
        }

        statusChangedCallbacks.push(this::onFollowerStatus) // become follower
    }

    private suspend fun onNodeConnected(conn: SimpleConnection) {
        connections.put(conn.other.id, conn)
    }

    private suspend fun onMessageReceived(connection: SimpleConnection, message: String) {
        val rpcType = getRpcType(message)

        when (rpcType) {
            RpcType.AppendEntries -> {
                val appendEntries = jacksonMapper.readValue<AppendEntries>(message)
                appendEntriesRequest(connection, appendEntries)
            }

            RpcType.AppendEntriesResp -> {
                val response = jacksonMapper.readValue<AppendEntriesResp>(message)
                appendEntriesResponse(response)
            }

            RpcType.RequestVote -> {
                val requestVote = jacksonMapper.readValue<RequestVote>(message)
                requestVoteRequest(connection, requestVote)
            }

            RpcType.RequestVoteResp -> {
                val response = jacksonMapper.readValue<RequestVoteResp>(message)
                requestVoteResponse(response)
            }

            RpcType.ConnectionCheck -> {}
        }
    }

    private suspend fun appendEntriesRequest(conn: SimpleConnection, appendEntries: AppendEntries) {
        val reply = appendEntries.term >= currentTerm
        // TODO: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm

        if (reply) {
            localTime = TimeSource.Monotonic.markNow()
            status = ServerStatus.FOLLOWER
        }

        val response = AppendEntriesResp(currentTerm, reply)
        val responseMessage = jacksonMapper.writeValueAsString(response)

        writerQueue.add(WriterEntry(conn, responseMessage))
    }

    private fun appendEntriesResponse(response: AppendEntriesResp) {
    }

    private suspend fun requestVoteRequest(conn: SimpleConnection, requestVote: RequestVote) {
        val grantVote = (requestVote.term >= currentTerm) &&
                (votedFor == null || votedFor == requestVote.candidateId) &&
                isLogIsUpToDate(requestVote)

        if (grantVote) {
            localTime = TimeSource.Monotonic.markNow()
            status = ServerStatus.FOLLOWER
        }

        val voteResponse = RequestVoteResp(currentTerm, grantVote)
        val voteResponseMessage = jacksonMapper.writeValueAsString(voteResponse)

        writerQueue.add(WriterEntry(conn, voteResponseMessage))
    }

    private fun requestVoteResponse(response: RequestVoteResp) {
        if (response.voteGranted)
            grantedVotes.incrementAndGet()
    }

    private suspend fun connectionCheck(conn: SimpleConnection) {
        val check = ConnectionCheck()
        val checkMessage = jacksonMapper.writeValueAsString(check)

        writerQueue.add(WriterEntry(conn, checkMessage))
    }

    private fun isLogIsUpToDate(requestVote: RequestVote): Boolean {
        // TODO: log list exclusion
        // Raft 5.4.1
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

    private fun getRpcType(message: String): RpcType {
        try {
            val messageTypeString = jacksonMapper.readTree(message).get("type")
            return jacksonMapper.treeToValue(messageTypeString, RpcType::class.java)
        }
        catch (e: Exception) {
            throw RuntimeException("Can not recognize type of received message: $message")
        }
    }
}