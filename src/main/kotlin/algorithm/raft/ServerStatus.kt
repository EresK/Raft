package algorithm.raft

enum class ServerStatus {
    FOLLOWER,
    CANDIDATE,
    LEADER
}