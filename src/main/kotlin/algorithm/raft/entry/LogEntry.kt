package algorithm.raft.entry

data class LogEntry(
    val command: Int,
    val term: Int
)