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
import ogr.raftv3.command.Command
import ogr.raftv3.command.CommandType
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
            post("/{type}") {
                val type = call.parameters["type"]!!.let { CommandType.fromString(it) }
                val key = call.request.queryParameters["key"]!!
                val value = call.request.queryParameters["value"]!!

                println("$type $key : $value") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                if (node.isLeader()) {
                    val result = node.appendCommand(Command(type, key, value))
                    call.respondText("Result: $result")
                }
                else call.respondText("Not Leader")
            }
            get("/machine") {
                val machineState = node.getMachineState()
                val result = machineState.toList().joinToString(prefix = "{", postfix = "}")

                if (node.isLeader())
                    call.respondText("Result: $result")
                else
                    call.respondText("Not Leader: $result")
            }
            get("/node") {
                val (entries, commitIndex) = node.state()
                val entriesResult = entries.joinToString(prefix = "{", postfix = "}")

                if (node.isLeader())
                    call.respondText("Commit index: $commitIndex\nResult: $entriesResult")
                else
                    call.respondText("Commit index: $commitIndex\nNot Leader: $entriesResult")
            }
        }
    }.start(true)

    node.stop()
    nodeJob.cancel()
}