package example

import kotlinx.coroutines.*
import tcp.MINUTE
import tcp.Node
import tcp.SECOND
import tcp.TcpLooper

fun main() = runBlocking {
    val self = Node(1, "127.0.0.1", 9001)
    val others = listOf(
        Node(2, "127.0.0.1", 9002),
        Node(3, "127.0.0.1", 9003)
    )

    val job = Job()
    val scope = CoroutineScope(Dispatchers.Default + job)

    val looper = TcpLooper(self, others)

    scope.launch {
        looper.start()
    }

    delay(SECOND * 10)

    looper.stop()

    delay(SECOND * 10)

    scope.cancel()

    println("scope isActive: ${scope.isActive}")
    println("job isActive: ${job.isActive}, isCompleted: ${job.isCompleted}, isCancelled: ${job.isCancelled}")

    delay(MINUTE * 5)
}