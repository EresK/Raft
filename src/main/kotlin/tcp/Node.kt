package tcp

import java.util.Objects

data class Node(
    val id: Int,
    val hostname: String,
    val port: Int
) {
    override fun equals(other: Any?): Boolean {
        return if (other is Node)
            id == other.id
        else
            false
    }

    override fun hashCode(): Int {
        return Objects.hash(id, hostname, port)
    }
}