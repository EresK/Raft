package algorithm.raft.entry

import algorithm.transport.SimpleConnection

data class WriterEntry(
    val connection: SimpleConnection,
    val message: String
)