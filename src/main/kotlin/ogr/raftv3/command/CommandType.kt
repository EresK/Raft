package ogr.raftv3.command

enum class CommandType {
    PUT,
    REMOVE,
    CLEAR;

    companion object {
        fun fromString(string: String): CommandType =
            when (string.lowercase()) {
                "put" -> PUT
                "remove" -> REMOVE
                "clear" -> CLEAR
                else -> throw IllegalArgumentException("String not match to any of the CommandType")
            }
    }
}
