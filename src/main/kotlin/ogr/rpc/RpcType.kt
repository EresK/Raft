package ogr.rpc

enum class RpcType {
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
    CheckConnection
}