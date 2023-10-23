package tcp

import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import kotlin.time.TimeSource

class TcpConnection(
    val self: Node,
    val other: Node,
    private val socket: Socket,
    private val writeChannel: ByteWriteChannel,
    private val readChannel: ByteReadChannel
) {
    companion object {
        private val DEFAULT_CHARSET = Charsets.UTF_8
        private const val MIN_BYTES_TO_READ = 4
    }

    var lastConnectionCheckTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()
        private set

    var status = TcpStatus.CONNECTED
        private set

    /**
     * @param message to send to another node
     * @return true if message sent or false otherwise
     * @author this code should run from one thread
     */
    suspend fun writeMessage(message: String): Boolean {
        if (socket.isClosed || status == TcpStatus.DISCONNECTED) {
            System.err.println("Node: ${self.id}, Can not write message to ${other.id} - socket is closed")
            return false
        }

        try {
            val messageArray = message.toByteArray(DEFAULT_CHARSET)
            val length = messageArray.size

            writeChannel.writeInt(length)
            writeChannel.writeFully(messageArray)

            lastConnectionCheckTime = TimeSource.Monotonic.markNow()

            println(" * write message: $message") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            return true
        }
        catch (e: Exception) {
            System.err.println("Node: ${self.id}, Can not write message to node ${other.id}")
            close()
        }

        return false
    }

    /**
     * @param callback on message received callback function
     * @return received message if it is available
     * @author this code should run from one thread
     */
    suspend fun readMessage(callback: (String) -> Unit = {}): String? {
        if (socket.isClosed || status == TcpStatus.DISCONNECTED) {
            System.err.println("Node: ${self.id}, Can not read message from ${other.id} - socket is closed")
            return null
        }

        var result: String? = null

        try {
            if (readChannel.availableForRead > MIN_BYTES_TO_READ) {
                val length = readChannel.readInt()
                val messageArray = ByteArray(length)

                readChannel.readFully(messageArray, 0, length)
                result = messageArray.toString(DEFAULT_CHARSET)

                lastConnectionCheckTime = TimeSource.Monotonic.markNow()

                callback(result)

                println(" * read message: $result") // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            }
        }
        catch (e: Exception) {
            System.err.println("Node: ${self.id}, Can not read message from node ${other.id}")
            close()
        }

        return result
    }

    fun close() {
        status = TcpStatus.DISCONNECTED
        socket.close()
    }
}