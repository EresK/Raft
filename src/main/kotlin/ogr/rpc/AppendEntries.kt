package ogr.rpc

import ogr.util.LogEntry

data class AppendEntries(
    val term: Int, // leader's term
    val leaderId: Int,
    val prevLogIndex: Int,
    val prevLogTerm: Int,
    val entries: List<LogEntry> = listOf(),
    val leaderCommit: Int, // leader's commit index
    val type: RpcType = RpcType.AppendEntries
)