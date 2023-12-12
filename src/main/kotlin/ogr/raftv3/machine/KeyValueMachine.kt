package ogr.raftv3.machine

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import ogr.raftv3.command.Command
import ogr.raftv3.command.CommandType

class KeyValueMachine {
    private val mutex = Mutex()
    private val map: MutableMap<String, String> = mutableMapOf()

    suspend fun state(): Map<String, String> {
        return mutex.withLock { map.toMap() }
    }

    suspend fun apply(command: Command) {
        mutex.withLock {
            when (command.type) {
                CommandType.PUT -> map[command.key] = command.value
                CommandType.REMOVE -> map.remove(command.key)
                CommandType.CLEAR -> map.clear()
            }
        }
    }
}