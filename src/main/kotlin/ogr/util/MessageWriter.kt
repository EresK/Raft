package ogr.util

import kotlinx.coroutines.delay
import ogr.transport.TcpConnection
import ogr.util.collection.CoroutineQueue

class MessageWriter {
    private val writerQueue: CoroutineQueue<MessageEntry> = CoroutineQueue()
    private var isCanceled = false

    companion object {
        private const val WRITE_LOOP_DELAY = 35L
    }

    suspend fun add(connection: TcpConnection, message: String) {
        writerQueue.add(MessageEntry(connection, message))
    }

    suspend fun addAll(elements: Collection<MessageEntry>) {
        writerQueue.addAll(elements)
    }

    suspend fun writeLoop() {
        while (!isCanceled) {
            val entry = writerQueue.poll()

            if (entry != null) {
                val connection = entry.connection
                val message = entry.message

                if (!connection.isClosed)
                    connection.write(message)
            }

            delay(WRITE_LOOP_DELAY)
        }
    }

    fun stop() {
        isCanceled = true
    }
}