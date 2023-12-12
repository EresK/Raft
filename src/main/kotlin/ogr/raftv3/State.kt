package ogr.raftv3

import ogr.raftv3.log.Log
import ogr.raftv3.log.LogEntry
import ogr.raftv3.machine.KeyValueMachine
import ogr.rpc.AppendEntries
import ogr.rpc.AppendEntriesResponse
import ogr.rpc.RequestVote
import ogr.rpc.RequestVoteResponse
import ogr.transport.Node
import java.util.concurrent.atomic.AtomicInteger

class State(val self: Node) {
    // Server state
    @Volatile
    var status = RaftStatus.FOLLOWER
        private set
    var leaderId: Int? = null
        private set
    val grantedVotes: AtomicInteger = AtomicInteger(0)
    val keyValueMachine = KeyValueMachine()

    // Persistent state
    @Volatile
    var currentTerm = 0
        private set
    private var votedFor: Int? = null
    val log: Log = Log()

    fun isLeader(): Boolean {
        return if (leaderId == null) false
        else leaderId == self.id
    }

    private fun updateStatus(newStatus: RaftStatus) {
        status = newStatus
    }

    fun becomeFollower() {
        votedFor = null
        updateStatus(RaftStatus.FOLLOWER)
    }

    fun prepareToCandidate() {
        updateStatus(RaftStatus.CANDIDATE)
    }

    fun becomeCandidate() {
        currentTerm += 1
        votedFor = self.id
        grantedVotes.set(1)
        updateStatus(RaftStatus.CANDIDATE)
    }

    fun becomeLeader() {
        votedFor = null
        leaderId = self.id
        updateStatus(RaftStatus.LEADER)
    }

    fun appendEntries(appendEntries: AppendEntries): AppendEntriesResponse {
        if (appendEntries.term < currentTerm)
            return AppendEntriesResponse(currentTerm, false)
        else { // appendEntries.term >= currentTerm
            // TODO on leader status?
            currentTerm = appendEntries.term
            votedFor = null
            leaderId = appendEntries.leaderId
            updateStatus(RaftStatus.FOLLOWER)
        }

        if (appendEntries.prevLogIndex != -1) {
            val logAtPrevLogIndex = log[appendEntries.prevLogIndex]
            val logValid = logAtPrevLogIndex?.let { it.term == appendEntries.prevLogTerm } ?: false

            if (!logValid)
                return AppendEntriesResponse(currentTerm, false)
        }

        var index = appendEntries.prevLogIndex + 1

        for (entry in appendEntries.entries) {
            if (log.isNotEmpty()) {
                val logConflict = log[index]?.let { it.term != entry.term } ?: false

                if (logConflict)
                    log.drop(index)
            }
            log[index] = entry
            index += 1
        }

        leaderId = appendEntries.leaderId
        log.commit(appendEntries.leaderCommit)

        return AppendEntriesResponse(currentTerm, true)
    }

    fun requestVote(requestVote: RequestVote): RequestVoteResponse {
        if (requestVote.term > currentTerm) {
            currentTerm = requestVote.term
            updateStatus(RaftStatus.FOLLOWER)
        }

        val logValid = validateLog(requestVote)

        val grantVote = when {
            currentTerm > requestVote.term || !logValid -> false

            currentTerm < requestVote.term -> {
                currentTerm = requestVote.term
                votedFor = requestVote.candidateId
                updateStatus(RaftStatus.FOLLOWER)
                true
            }

            // currentTerm == requestVote.term
            else -> {
                if (votedFor == null && status != RaftStatus.LEADER) {
                    votedFor = requestVote.candidateId
                }
                votedFor == requestVote.candidateId
            }
        }

        return RequestVoteResponse(currentTerm, grantVote)
    }

    fun appendEntry(entry: LogEntry): Int {
        return log.append(entry)
    }

    suspend fun applyCommand() {
        if (log.apply()) {
            val entry = log[log.lastApplied]!!
            keyValueMachine.apply(entry.command)
        }
    }

    private fun validateLog(requestVote: RequestVote): Boolean {
        val lastTerm = log.lastTerm()
        val lastIndex = log.lastIndex()

        val leaderLogLastTermIsBigger = lastTerm?.let { it < requestVote.lastLogTerm } ?: false

        if (leaderLogLastTermIsBigger)
            return true

        val leaderLogLastTermEqual = lastTerm?.let { it == requestVote.lastLogTerm } ?: true
        val leaderLogLongerOrEqual = lastIndex <= requestVote.lastLogIndex

        return leaderLogLastTermEqual && leaderLogLongerOrEqual
    }
}