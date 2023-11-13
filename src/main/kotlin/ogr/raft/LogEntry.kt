package ogr.raft

data class LogEntry(
    val command: Int,
    val term: Int
)