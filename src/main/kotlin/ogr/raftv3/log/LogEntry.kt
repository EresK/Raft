package ogr.raftv3.log

import ogr.raftv3.command.Command
import java.util.*

data class LogEntry(
    val command: Command,
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
        return "($command, term: $term)"
    }
}