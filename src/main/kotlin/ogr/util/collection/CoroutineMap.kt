package ogr.util.collection

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class CoroutineMap<K, V> {
    private val mutex = Mutex()
    private val map: MutableMap<K, V> = mutableMapOf()

    suspend fun getSize(): Int {
        return mutex.withLock {
            map.size
        }
    }

    suspend fun getEntries(): MutableSet<MutableMap.MutableEntry<K, V>> {
        return mutex.withLock {
            map.entries
        }
    }

    suspend fun getKeys(): MutableSet<K> {
        return mutex.withLock {
            map.keys
        }
    }

    suspend fun getValues(): MutableCollection<V> {
        return mutex.withLock {
            map.values
        }
    }

    suspend fun containsKey(key: K): Boolean {
        return mutex.withLock {
            map.containsKey(key)
        }
    }

    suspend fun containsValue(value: V): Boolean {
        return mutex.withLock {
            map.containsValue(value)
        }
    }

    suspend fun get(key: K): V? {
        return mutex.withLock {
            map[key]
        }
    }

    suspend fun clear() {
        mutex.withLock {
            map.clear()
        }
    }

    suspend fun isEmpty(): Boolean {
        return mutex.withLock {
            map.isEmpty()
        }
    }

    suspend fun remove(key: K): V? {
        return mutex.withLock {
            map.remove(key)
        }
    }

    suspend fun putAll(from: Map<out K, V>) {
        mutex.withLock {
            map.putAll(from)
        }
    }

    suspend fun put(key: K, value: V): V? {
        return mutex.withLock {
            map.put(key, value)
        }
    }

}