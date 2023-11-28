package ogr.raftv3

enum class RaftStatus {
    FOLLOWER,
    CANDIDATE,
    LEADER
}