package ogr.server

import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import ogr.raftv3.RaftNode
import ogr.transport.Node

fun main(): Unit = runBlocking {
    val self = Node(1, "127.0.0.1", 9001)
    val others = listOf(
        Node(2, "127.0.0.1", 9002),
        Node(3, "127.0.0.1", 9003),
    )

    val node = RaftNode(self, others)
    val nodeJob = CoroutineScope(coroutineContext + Dispatchers.Default).launch {
        node.start()
    }

    embeddedServer(Netty, port = 8081) {
        routing {
            post("/{command}") {
                val command = call.parameters["command"]!!
                val commandNumber = command.toIntOrNull() ?: 0

                if (node.isLeader()) {
                    val result = node.appendCommand(commandNumber)
                    call.respondText("Result: $result")
                }
                else call.respondText("Follower")
            }
            get {
                val entries = node.state()
                val entriesResult = entries.joinToString(prefix = "[", postfix = "]")

                if (node.isLeader())
                    call.respondText("Result: $entriesResult")
                else
                    call.respondText("Follower: $entriesResult")
            }
        }
    }.start(true)

    node.stop()
    nodeJob.cancel()
}