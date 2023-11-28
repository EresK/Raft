package ogr.transport

import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import kotlinx.coroutines.delay
import kotlin.time.TimeSource

class TcpConnection(
    val self: Node,
    val other: Node,
    private val socket: Socket,
    private val onMessageReceived: suspend (TcpConnection, String) -> Unit,
    private var readChannel: ByteReadChannel? = null,
    private var writeChannel: ByteWriteChannel? = null
) {
    var isClosed = false
        private set

    var lastConnectionTime = TimeSource.Monotonic.markNow()
        private set

    companion object {
        private const val MIN_BYTES_TO_READ = 4L
        private const val READ_LOOP_DELAY = 35L
    }

    init {
        readChannel = readChannel ?: socket.openReadChannel()
        writeChannel = writeChannel ?: socket.openWriteChannel(autoFlush = false)
    }

    suspend fun readLoop() {
        while (!isClosed) {
            if (readChannel!!.availableForRead > MIN_BYTES_TO_READ)
                read()
            delay(READ_LOOP_DELAY)
        }
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
            System.err.println("Read error, while reading from node ${other.id}, connection closed")
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
            System.err.println("Write error, while writing to node ${other.id}, connection closed")
            close()
        }
    }

    private fun close() {
        isClosed = true
        socket.close()
    }
}