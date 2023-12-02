package ogr.util

import java.util.*

data class LogEntry(
    val command: Int,
    val term: Int
) {
    override fun equals(other: Any?): Boolean {
        return if (other is LogEntry)
            (command == other.command) && (term == other.term)
        else
            false
    }

    override fun hashCode(): Int {
        return Objects.hash(command, term)
    }

    override fun toString(): String {
        return "(cmd: $command, term: $term)"
    }
}