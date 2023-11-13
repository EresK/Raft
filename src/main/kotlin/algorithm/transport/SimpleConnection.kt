package algorithm.transport

import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import kotlinx.coroutines.delay
import kotlin.time.TimeSource

class SimpleConnection(
    val self: Node,
    val other: Node,
    private val socket: Socket,
    private val onMessageReceived: suspend (SimpleConnection, String) -> Unit,
    private var readChannel: ByteReadChannel? = null,
    private var writeChannel: ByteWriteChannel? = null
) {
    var isClosed = false
        private set

    var lastConnectionTime = TimeSource.Monotonic.markNow()
        private set

    companion object {
        private const val MIN_BYTES_TO_READ = 4L
        private const val READING_LOOP_DELAY = 30L
    }

    init {
        readChannel = readChannel ?: socket.openReadChannel()
        writeChannel = writeChannel ?: socket.openWriteChannel(autoFlush = false)
    }

    private suspend fun read() {
        try {
            val length = readChannel!!.readInt()
            val byteArray = ByteArray(length)

            readChannel!!.readFully(byteArray, 0, length)
            val message = byteArray.toString(Charsets.UTF_8)

            lastConnectionTime = TimeSource.Monotonic.markNow()

            onMessageReceived(this, message)
        }
        catch (e: Exception) {
            System.err.println("Node: ${self.id}, Can not read message from node ${other.id}")
            close()
        }
    }

    suspend fun write(message: String) {
        try {
            val byteArray = message.toByteArray(Charsets.UTF_8)
            val length = byteArray.size

            writeChannel!!.writeInt(length)
            writeChannel!!.writeFully(byteArray)
            writeChannel!!.flush()

            lastConnectionTime = TimeSource.Monotonic.markNow()
        }
        catch (e: Exception) {
            System.err.println("Node: ${self.id}, Can not write message to node ${other.id}")
            close()
        }
    }

    suspend fun readingLoop() {
        while (!isClosed) {
            if (readChannel!!.availableForRead > MIN_BYTES_TO_READ)
                read()
            delay(READING_LOOP_DELAY)
        }
    }

    private fun close() {
        isClosed = true
        socket.close()
    }
}