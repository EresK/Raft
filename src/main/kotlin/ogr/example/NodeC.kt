package ogr.example

import kotlinx.coroutines.runBlocking
import ogr.transport.Node
import ogr.transport.TcpConnection
import ogr.transport.TcpLooper

fun main(): Unit = runBlocking {
    val self = Node(3, "127.0.0.1", 9003)
    val others = listOf(
        Node(2, "127.0.0.1", 9002),
        Node(1, "127.0.0.1", 9001)
    )

    val nodes: MutableList<TcpConnection> = mutableListOf()

    val looper = TcpLooper(self, others,
        { connection ->
            nodes.add(connection)
        },
        { connection, message ->

        },
        { connection ->
            connection.write("hello")
        })
    looper.start()
}