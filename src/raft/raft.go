package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int32
	Command interface{}
}

type roleType uint8

const (
	leader    roleType = 1
	follower  roleType = 2
	candidate roleType = 3
)

func (v roleType) String() string {
	switch v {
	case leader:
		return "leader"
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	default:
		return "unknown"
	}
}

func (ele *LogEntry) String() string {
	return fmt.Sprintf("{Term:%v,Idx:%v,Command:%v} ", ele.Term, ele.Index, cmd2Str(ele.Command))
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role      roleType
	heartbeat chan struct{}

	applyCh chan ApplyMsg

	currentTerm int32
	votedFor    *int32

	//  each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	log          []*LogEntry
	lastLogIndex int32

	commitIndex int32
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) setRole(role roleType) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}
func (rf *Raft) getRole() roleType {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}
func (rf *Raft) setTerm(term int32) {
	atomic.StoreInt32(&rf.currentTerm, term)
}
func (rf *Raft) getTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}
func (rf *Raft) incrTerm() {
	atomic.AddInt32(&rf.currentTerm, 1)
}
func (rf *Raft) setVoteFor(voteFor *int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = voteFor
}
func (rf *Raft) getVoteFor() *int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}
func (rf *Raft) majorityNum() int32 {
	return int32(len(rf.peers)/2 + 1)
}
func (rf *Raft) getLastLogIdx() int {
	return int(atomic.LoadInt32(&rf.lastLogIndex))
}
func (rf *Raft) setLastLogIdx(idx int) {
	atomic.StoreInt32(&rf.lastLogIndex, int32(idx))
}
func (rf *Raft) getLastLogTerm() int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIdx := atomic.LoadInt32(&rf.lastLogIndex)
	return rf.log[lastLogIdx].Term
}
func (rf *Raft) setLogAt(idx int, entry *LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log[idx] = entry
}
func (rf *Raft) appendLog(entry *LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log[atomic.AddInt32(&rf.lastLogIndex, 1)] = entry
}
func (rf *Raft) subLogEntry(i, j int) []*LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[i:j]
}
func (rf *Raft) getLogEntry(idx int) *LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[idx]
}
func (rf *Raft) setNextIndex(at, val int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[at] = val
}
func (rf *Raft) nextIndexAt(idx int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[idx]
}
func (rf *Raft) getNextIndex() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dup := make([]int, len(rf.nextIndex))
	copy(dup, rf.nextIndex)
	return dup
}
func (rf *Raft) setMatchIndex(at, val int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[at] = val
}
func (rf *Raft) matchIndexAt(at int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[at]
}
func (rf *Raft) getMatchIndex() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dup := make([]int, len(rf.matchIndex))
	copy(dup, rf.matchIndex)
	return dup
}
func (rf *Raft) setCmtIdx(idx int) {
	atomic.StoreInt32(&rf.commitIndex, int32(idx))
}
func (rf *Raft) getCmtIdx() int {
	return int(atomic.LoadInt32(&rf.commitIndex))
}
func (rf *Raft) debugf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[node:%v][term:%v][role:%v][lastLogIdx:%v][lastLogTerm:%v]",
		rf.me, rf.getTerm(), rf.getRole(), rf.getLastLogIdx(), rf.getLastLogTerm())
	content := fmt.Sprintf(format, a...)
	DPrintf("%s %s", prefix, content)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.getTerm()), rf.getRole() == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32 // candidate’s term
	CandidateId  int32 // candidate requesting vote
	LastLogIndex int   // index of candidate’s last log entry (§5.4)
	LastLogTerm  int32 // term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 // currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int32       // leader term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int32       // term of prevLogIndex entry
	Entries      []*LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex

}

type AppendEntriesReply struct {
	Term    int32 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.getTerm() {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.getTerm() {
		rf.setVoteFor(nil)
		rf.setRole(follower)
		rf.setTerm(args.Term)
	}
	// Raft determines which of two logs is more up-to-date by comparing the index
	// and term of the last entries in the logs. If the logs have last entries with
	// different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	if (rf.getVoteFor() == nil || *rf.getVoteFor() == args.CandidateId) &&
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIdx() ||
			args.LastLogTerm > rf.getLastLogTerm()) {
		rf.debugf("grant vote to %v", args.CandidateId)
		reply.VoteGranted = true
		rf.setVoteFor(&args.CandidateId)
		return
	}
}

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		if args.LeaderCommit > rf.getCmtIdx() {
			rf.setCmtIdx(min(args.LeaderCommit, rf.getLastLogIdx()))
		}
	}()
	if args.Term < rf.getTerm() {
		reply.Success = false
		return
	}
	if args.Term > rf.getTerm() {
		rf.setVoteFor(nil)
		rf.setTerm(args.Term)
		rf.setRole(follower)
	}
	if len(args.Entries) == 0 {
		rf.heartbeat <- struct{}{}
		return
	}
	if args.PrevLogIndex > rf.getLastLogIdx() ||
		rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		rf.debugf("log conflict at prevLogIndex, drop")
		reply.Success = false
		return
	}
	for idx, entry := range args.Entries {
		rf.setLogAt(args.PrevLogIndex+1+idx, entry)
	}
	rf.setLastLogIdx(args.PrevLogIndex + len(args.Entries))
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	defer func() {
		if reply.Term > rf.getTerm() {
			rf.setTerm(reply.Term)
			rf.setRole(follower)
		}
	}()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	defer func() {
		if reply.Term > rf.getTerm() {
			rf.setTerm(reply.Term)
			rf.setRole(follower)
		}
	}()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	if rf.getRole() != leader {
		return index, int(rf.getTerm()), false
	}
	rf.debugf("append %v", cmd2Str(command))
	entry := &LogEntry{
		Term:    rf.getTerm(),
		Command: command,
		Index:   rf.getLastLogIdx() + 1,
	}
	rf.appendLog(entry)
	index = rf.getLastLogIdx()
	term = int(rf.getTerm())
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.getCmtIdx() > rf.lastApplied {
			rf.lastApplied++
			entry := rf.getLogEntry(rf.lastApplied)
			rf.debugf("apply %v", entry)
			rf.applyCh <- ApplyMsg{
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandValid: true,
			}
		}
		switch rf.getRole() {
		case leader:
			rf.sendHeartbeat()
			rf.duplicateLog()
			rf.commitLog()
			time.Sleep(50 * time.Millisecond)
		case follower:
			select {
			case <-time.After(randomElectionTimeout()):
				rf.debugf("%s", "heartbeat timeout, become candidate")
				rf.setRole(candidate)
			case <-rf.heartbeat:
				// rf.debugf("%s", "receive heartbeat, keep follower")
				// 保持follower
			}
		case candidate:
			rf.election()
		default:
		}

	}
}
func (rf *Raft) election() {
	rf.incrTerm()
	var voteGot int32
	win := make(chan struct{})
	// 给自己投票
	voteForVal := int32(rf.me)
	rf.setVoteFor(&voteForVal)
	atomic.AddInt32(&voteGot, 1)
	// 并行向他人发起投票请求
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			args := &RequestVoteArgs{
				Term:         rf.getTerm(),
				CandidateId:  int32(rf.me),
				LastLogIndex: rf.getLastLogIdx(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			var callSucc bool
			for !callSucc && rf.getRole() == candidate {
				callSucc = rf.sendRequestVote(server, args, reply)
			}
			if reply.VoteGranted && atomic.AddInt32(&voteGot, 1) >= rf.majorityNum() {
				win <- struct{}{}
			}
		}(server)
	}
	// 发生三种情况之一停止本轮选举:
	// 1. 赢得选举 2.收到心跳 3.选举超时
	select {
	case <-win:
		rf.debugf("become leader")
		rf.setRole(leader)
		for server := 0; server < len(rf.peers); server++ {
			rf.setNextIndex(server, rf.getLastLogIdx()+1)
		}
	case <-rf.heartbeat:
		rf.setRole(follower)
	case <-time.After(500 * time.Millisecond):
		// 选举超时
		rf.debugf("election timeout")
	}
}
func (rf *Raft) sendHeartbeat() {
	args := &AppendEntriesArgs{
		Term:         rf.getTerm(),
		LeaderId:     rf.me,
		Entries:      nil,
		LeaderCommit: rf.getCmtIdx(),
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			var callSucc bool
			for !callSucc && rf.getRole() == leader {
				callSucc = rf.sendAppendEntries(server, args, &AppendEntriesReply{})
			}
		}(server)
	}
}
func (rf *Raft) duplicateLog() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			lastLogIdx := rf.getLastLogIdx()
			nextIdxAtServer := rf.nextIndexAt(server)
			if lastLogIdx < nextIdxAtServer {
				return
			}
			args := &AppendEntriesArgs{
				Term:         rf.getTerm(),
				LeaderId:     rf.me,
				PrevLogIndex: nextIdxAtServer - 1,
				PrevLogTerm:  rf.getLogEntry(nextIdxAtServer - 1).Term,
				Entries:      rf.subLogEntry(nextIdxAtServer, lastLogIdx+1),
				LeaderCommit: rf.getCmtIdx(),
			}
			reply := &AppendEntriesReply{}
			var callSucc bool
			for !callSucc && rf.getRole() == leader {
				callSucc = rf.sendAppendEntries(server, args, reply)
			}
			if !reply.Success { // nextIndex 自减并重试
				rf.setNextIndex(server, nextIdxAtServer-1)
			}
			rf.debugf("send %v to %v success", args.Entries, server)
			rf.setMatchIndex(server, lastLogIdx)
			rf.setNextIndex(server, lastLogIdx+1)
		}(server)
	}
}
func (rf *Raft) commitLog() {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for N := rf.getLastLogIdx(); N > rf.getCmtIdx(); N-- {
		if rf.getLogEntry(N).Term != rf.getTerm() {
			break
		}
		var cnt int32
		for _, matchIdx := range rf.getMatchIndex() {
			if matchIdx >= N {
				cnt++
			}
		}
		// rf.debugf("N=%v, cnt=%v", N, cnt)
		if cnt >= rf.majorityNum() {
			rf.debugf("update commitIndex to %v", N)
			rf.setCmtIdx(N)
			break
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	peerCnt := len(peers)
	rf.heartbeat = make(chan struct{})
	rf.applyCh = applyCh
	rf.setRole(follower)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]*LogEntry, 128)
	rf.log[0] = &LogEntry{}
	rf.matchIndex = make([]int, peerCnt)
	rf.nextIndex = make([]int, peerCnt)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
