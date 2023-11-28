package ogr.util

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import ogr.rpc.RpcType
import ogr.transport.TcpConnection
import ogr.util.collection.CoroutineQueue

class MessageQueue {
    private val messageQueue: CoroutineQueue<MessageEntry> = CoroutineQueue()
    private val mapper = jacksonObjectMapper()

    suspend fun add(connection: TcpConnection, message: String) {
        messageQueue.add(MessageEntry(connection, message))
    }

    suspend fun poll(): ReceivedMessage? {
        val messageEntry = messageQueue.poll()
        return getReceivedMessageOrNull(messageEntry)
    }

    suspend fun peek(): ReceivedMessage? {
        val messageEntry = messageQueue.peek()
        return getReceivedMessageOrNull(messageEntry)
    }

    private fun getReceivedMessageOrNull(messageEntry: MessageEntry?): ReceivedMessage? {
        return if (messageEntry != null) {
            val connection = messageEntry.connection
            val message = messageEntry.message
            val rpcType = getRpcType(message)

            return ReceivedMessage(connection, message, rpcType)
        }
        else
            null
    }

    private fun getRpcType(message: String): RpcType {
        try {
            val messageTypeString = mapper.readTree(message).get("type")
            return mapper.treeToValue(messageTypeString, RpcType::class.java)
        }
        catch (e: Exception) {
            throw RuntimeException("Can not recognize rpc type: $message")
        }
    }
}