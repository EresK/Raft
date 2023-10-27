package example

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import tcp.MINUTE
import tcp.Node
import tcp.TcpLooper

fun main() = runBlocking {
    val self = Node(3, "127.0.0.1", 9003)
    val others = listOf(
        Node(1, "127.0.0.1", 9001),
        Node(2, "127.0.0.1", 9002)
    )

    val looper = TcpLooper(self, others)
    looper.start()

    delay(MINUTE * 5)
}