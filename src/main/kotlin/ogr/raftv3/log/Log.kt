package ogr.raftv3.log

import ogr.util.LogEntry
import kotlin.math.min

class Log {
    private var _entries: MutableList<LogEntry> = mutableListOf()
    val entries: List<LogEntry> = _entries

    // Volatile state
    var commitIndex = -1
        private set

    var lastApplied = -1
        private set

    fun lastIndex(): Int = _entries.size - 1

    fun lastTerm(): Int? = _entries.lastOrNull()?.term

    fun isNotEmpty(): Boolean = _entries.isNotEmpty()

    fun lastStartsAt(index: Int): List<LogEntry> {
        return _entries.filterIndexed { idx, _ -> idx >= index }
    }

    fun append(entry: LogEntry): Int {
        _entries.add(entry)
        return _entries.size - 1
    }

    fun commit(index: Int): Boolean {
        val idx = min(lastIndex(), index)
        commitIndex = idx
        return true
    }

    fun drop(startIndex: Int) {
        val drop = _entries.size - startIndex
        _entries = _entries.dropLast(drop).toMutableList()
    }

    operator fun get(prevLogIndex: Int): LogEntry? {
        return _entries.getOrNull(prevLogIndex)
    }

    operator fun set(index: Int, entry: LogEntry) {
        if (index == _entries.size)
            _entries.add(entry)
        else
            _entries[index] = entry
    }
}