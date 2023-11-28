package ogr.raft

enum class RaftStatus {
    FOLLOWER,
    CANDIDATE,
    LEADER
}