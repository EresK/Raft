package ogr.util.collection

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*

class CoroutineQueue<E> {
    private val mutex = Mutex()
    private val queue: Queue<E> = LinkedList()

    suspend fun getSize(): Int {
        return mutex.withLock {
            queue.size
        }
    }

    suspend fun add(element: E): Boolean {
        return mutex.withLock {
            queue.add(element)
        }
    }

    suspend fun addAll(elements: Collection<E>): Boolean {
        return mutex.withLock {
            queue.addAll(elements)
        }
    }

    suspend fun clear() {
        mutex.withLock {
            queue.clear()
        }
    }

    suspend fun remove(): E {
        return mutex.withLock {
            queue.remove()
        }
    }

    suspend fun retainAll(elements: Collection<E>): Boolean {
        return mutex.withLock {
            queue.retainAll(elements.toSet())
        }
    }

    suspend fun removeAll(elements: Collection<E>): Boolean {
        return mutex.withLock {
            queue.retainAll(elements.toSet())
        }
    }

    suspend fun remove(element: E): Boolean {
        return mutex.withLock {
            queue.remove(element)
        }
    }

    suspend fun isEmpty(): Boolean {
        return mutex.withLock {
            queue.isEmpty()
        }
    }

    suspend fun poll(): E? {
        return mutex.withLock {
            queue.poll()
        }
    }

    suspend fun element(): E {
        return mutex.withLock {
            queue.element()
        }
    }

    suspend fun peek(): E? {
        return mutex.withLock {
            queue.peek()
        }
    }

    suspend fun offer(p0: E): Boolean {
        return mutex.withLock {
            queue.offer(p0)
        }
    }

    suspend fun containsAll(elements: Collection<E>): Boolean {
        return mutex.withLock {
            queue.containsAll(elements)
        }
    }

    suspend fun contains(element: E): Boolean {
        return mutex.withLock {
            queue.contains(element)
        }
    }
}