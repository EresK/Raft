package ogr.raftv3.command

data class Command(
    val type: CommandType,
    val key: String,
    val value: String
) {
    override fun toString(): String {
        return "$type:$key:$value"
    }
}
