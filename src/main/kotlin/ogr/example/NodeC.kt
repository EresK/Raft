package ogr.example

import kotlinx.coroutines.runBlocking
import ogr.raft.RaftServer
import ogr.transport.Node

fun main(): Unit = runBlocking {
    val self = Node(3, "127.0.0.1", 9003)
    val others = listOf(
        Node(2, "127.0.0.1", 9002),
        Node(1, "127.0.0.1", 9001)
    )

    val server = RaftServer(self, others)
    server.start()
}