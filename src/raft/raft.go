package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"labrpc"
)

func init() {
	gob.Register(LogEntry{})
	gob.Register(int(0))
}

// Ticker pacing: steady leader heartbeats (≤10/s) vs short follower/candidate polls.
const (
	heartbeatInterval    = 100 * time.Millisecond
	electionPollInterval = 10 * time.Millisecond

	follower  = 0
	leader    = 1
	candidate = 2
)

// LogEntry is one Raft log entry (Figure 2). Command is unused until 3B.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// ApplyMsg carries a committed Raft entry to the service or tester on applyCh.
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg
	dead    int32 // set by Kill(); accessed via atomic ops

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int // -1 means none
	log         []LogEntry
	commitIndex int // Idx of highest log entry known to be committed by consensus
	lastApplied int // Idx of highest log entry applied to state machine

	lastIncludedIndex int
	lastIncludedTerm  int

	serverState int // follower, leader, candidate

	//Volatile state on leaders
	nextIndex  []int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	electionDeadline time.Time
	votesReceived    int // votes granted in current election (candidate only); guarded by mu
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// HEARTBEAT msg
// Sent from leader to follower, resets followers timeout. when recieved by follower
type AppendEntriesArgs struct {
	Term int      // leader's term (follower uses this to update / reject stale RPCs)
	Log  LogEntry //Empty if just hearbeat

	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry //entries to store (empty for heartbeat)
	LeaderCommit int        // Leader commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Fast backup hint (used when Success==false).
	// XTerm:  term of the conflicting entry (-1 if follower log is too short).
	// XIndex: index of the first entry in XTerm in the follower's log.
	// XLen:   length of the follower's log (used when XTerm==-1).
	XTerm  int
	XIndex int
	XLen   int
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	// Stale leader.
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.serverState = follower

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	reply.Term = rf.currentTerm
	rf.log = []LogEntry{}

	rf.persist()
	rf.persister.SaveSnapshot(args.Data)

	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		Index:       rf.lastIncludedIndex,
		UseSnapshot: true,
		Snapshot:    args.Data,
	}

}

func (rf *Raft) RaftSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.serverState == leader
	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var log []LogEntry
	if d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil || d.Decode(&log) != nil {
		return
	}
	rf.mu.Lock()
	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
	rf.log = log
	rf.mu.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()

	if index > rf.lastApplied || index <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	rf.persister.SaveSnapshot(snapshot)

	//Index from which we need to cut.
	cutIdx := index - rf.lastIncludedIndex

	rf.lastIncludedIndex = index

	if cutIdx > 0 {
		rf.lastIncludedTerm = rf.log[cutIdx-1].Term
	}
	rf.log = rf.log[cutIdx:]
	rf.persist()

	rf.mu.Unlock()

}

func randomElectionTimeout() int64 {
	return 250 + (rand.Int63() % 300)
}

// Raft index of the last entry in the log
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// lastLogTerm returns the term of the last entry in the log.
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// advanceCommit sets commitIndex to the largest index > old commitIndex that is stored
// on a strict majority and was created in the current leader term (Figure 2).
// Caller must hold rf.mu.
func (rf *Raft) advanceCommit() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if n == rf.lastIncludedIndex {
			if rf.lastIncludedTerm != rf.currentTerm {
				continue
			}
		} else if rf.log[n-rf.lastIncludedIndex-1].Term != rf.currentTerm {
			continue
		}
		nAgree := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				nAgree++
			}
		}
		if nAgree > len(rf.peers)/2 {
			rf.commitIndex = n
			break
		}
	}
}

// processAppendEntriesReply updates nextIndex/matchIndex and possibly commitIndex after
// receiving an AppendEntries reply. Caller must hold rf.mu.
func (rf *Raft) processAppendEntriesReply(peer int, rpcTerm int, prevI int, ent []LogEntry, reply *AppendEntriesReply) {
	if rf.currentTerm != rpcTerm || rf.serverState != leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.serverState = follower
		rf.votedFor = -1
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
		rf.persist()
		return
	}
	if reply.Success {
		newMatch := prevI
		if len(ent) > 0 {
			newMatch = prevI + len(ent)
		}
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		rf.nextIndex[peer] = newMatch + 1
		rf.advanceCommit()
		return
	}
	if reply.XTerm == -1 {
		// Follower log was too short; jump nextIndex to end of follower's log.
		rf.nextIndex[peer] = reply.XLen
	} else {
		// Find the last index in our log that has XTerm.
		newNext := reply.XIndex
		for i := len(rf.log) - 1; i >= 1; i-- {
			if rf.log[i].Term == reply.XTerm {
				newNext = i + 1
				break
			}
		}
		rf.nextIndex[peer] = newNext
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Stale leader — reject (Figure 2).
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.serverState = follower

	lastLogIndex := rf.lastLogIndex()
	if args.PrevLogIndex > lastLogIndex {
		// Follower log is too short.
		reply.XTerm = -1
		reply.XLen = lastLogIndex + 1
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
		return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
		return
	}
	prevTerm := rf.lastLogTerm()

	if prevTerm != args.PrevLogTerm {

		if args.PrevLogIndex <= rf.lastIncludedIndex {
			rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
			return
		}

		xTerm := rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
		xIndex := args.PrevLogIndex
		for xIndex > rf.lastIncludedIndex+1 {
			if rf.log[xIndex-1-rf.lastIncludedIndex-1].Term != xTerm {
				break
			}
			xIndex--
		}
		reply.XTerm = xTerm
		reply.XIndex = xIndex
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
		return
	}

	cut := args.PrevLogIndex - rf.lastIncludedIndex
	if len(args.Entries) > 0 {
		rf.log = rf.log[:cut]
		rf.log = append(rf.log, args.Entries...)
	} else if lastLogIndex > args.PrevLogIndex {
		// Heartbeat — drop divergent suffix.
		rf.log = rf.log[:cut]
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}
	// Truncation can shrink the log while commitIndex stayed high; always clamp.
	lastNew := rf.lastLogIndex()
	if rf.commitIndex > lastNew {
		rf.commitIndex = lastNew
	}
	if rf.lastApplied > lastNew {
		rf.lastApplied = lastNew
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.persist()
	rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Stale term — reject (Figure 2).
	if args.Term < rf.currentTerm {
		return
	}

	// At least as new as us then adopt term and become follower if we were candidate/leader.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.serverState = follower
	}

	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()

	upToDate := true
	upToDate = args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
	}
	rf.persist()
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) handleSendInstallSnapshot(peer int) {
	rf.mu.Lock()
	if rf.serverState != leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	lastIdx := rf.lastIncludedIndex
	lastTerm := rf.lastIncludedTerm
	leaderId := rf.me
	snap := rf.persister.ReadSnapshot()
	rf.mu.Unlock()

	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderId:          leaderId,
		LastIncludedIndex: lastIdx,
		LastIncludedTerm:  lastTerm,
		Data:              snap,
	}
	reply := &InstallSnapshotReply{}

	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != term || rf.serverState != leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.serverState = follower
		rf.votedFor = -1
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
		rf.persist()
		return
	}

	if reply.Term < rf.currentTerm {
		return
	}

	if lastIdx > rf.matchIndex[peer] {
		rf.matchIndex[peer] = lastIdx
	}
	if rf.nextIndex[peer] < lastIdx+1 {
		rf.nextIndex[peer] = lastIdx + 1
	}

	rf.advanceCommit()

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})

	index := rf.lastLogIndex()
	rf.matchIndex[rf.me] = index

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		peer := i
		next := rf.nextIndex[peer]
		if next <= rf.lastIncludedIndex {
			go rf.handleSendInstallSnapshot(peer)
			continue
		}
		prevIdx := next - 1
		
		//TODO: TRY LAST LOG TERM
		prevTerm := rf.lastIncludedTerm
		if prevIdx > rf.lastIncludedIndex {
			prevTerm = rf.log[prevIdx-rf.lastIncludedIndex-1].Term
		}
		entries := append([]LogEntry(nil), rf.log[next-rf.lastIncludedIndex-1:]...)
		leaderCommit := rf.commitIndex
		term := rf.currentTerm
		go func(p, prevI, prevT, lc, t int, ent []LogEntry) {
			args := &AppendEntriesArgs{
				Term:         t,
				PrevLogIndex: prevI,
				PrevLogTerm:  prevT,
				Entries:      ent,
				LeaderCommit: lc,
			}
			reply := &AppendEntriesReply{}
			ok := rf.peers[p].Call("Raft.AppendEntries", args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendEntriesReply(p, term, prevI, ent, reply)

		}(peer, prevIdx, prevTerm, leaderCommit, term, entries)
	}

	rf.persist()
	return index, rf.currentTerm, true

}

func (rf *Raft) initLeaderIndex() {
	//TODO: Set nextIndex and lastIndex vars for leader upon election

	lastLogIndex := rf.lastLogIndex()
	next := lastLogIndex + 1
	for j := range rf.peers {
		rf.nextIndex[j] = next
		rf.matchIndex[j] = 0
	}
	rf.matchIndex[rf.me] = lastLogIndex

}

// doElection starts an election if the deadline has passed. Does not hold rf.mu across RPCs.
func (rf *Raft) doElection() {
	rf.mu.Lock()
	if rf.serverState == leader {
		rf.mu.Unlock()
		return
	}
	if rf.electionDeadline.IsZero() {
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
		rf.mu.Unlock()
		return
	}
	if !time.Now().After(rf.electionDeadline) {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.serverState = candidate
	rf.votedFor = rf.me
	rf.votesReceived = 1
	term := rf.currentTerm
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	rf.persist()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
	rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		peer := i
		go func() {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != term || rf.serverState != candidate {
				return
			}
			if ok && reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.serverState = follower
				rf.votedFor = -1
				rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
				rf.persist()
				return
			}
			if ok && reply.VoteGranted {
				rf.votesReceived++
				if rf.votesReceived > len(rf.peers)/2 {
					rf.serverState = leader
					rf.initLeaderIndex()
				}
			}
		}()
	}
}

func (rf *Raft) tickFollower() {
	rf.mu.Lock()
	if rf.electionDeadline.IsZero() {
		rf.electionDeadline = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
		rf.mu.Unlock()
		return
	}
	st := rf.serverState
	timedOut := (st == follower || st == candidate) && time.Now().After(rf.electionDeadline)
	rf.mu.Unlock()
	if timedOut {
		rf.doElection()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		st := rf.serverState
		rf.mu.Unlock()

		if st == leader {
			rf.mu.Lock()
			term := rf.currentTerm
			lc := rf.commitIndex
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				peer := i
				next := rf.nextIndex[peer]
				if next <= rf.lastIncludedIndex {
					go rf.handleSendInstallSnapshot(peer)
					continue
				}
				prevI := next - 1
				prevT := rf.lastIncludedTerm
				if prevI > rf.lastIncludedIndex {
					prevT = rf.log[prevI-rf.lastIncludedIndex-1].Term
				}
				entries := append([]LogEntry(nil), rf.log[next-rf.lastIncludedIndex-1:]...)
				go func(p, pi, pt, lcCopy, rpcTerm int, ent []LogEntry) {
					args := &AppendEntriesArgs{
						Term:         rpcTerm,
						PrevLogIndex: pi,
						PrevLogTerm:  pt,
						Entries:      ent,
						LeaderCommit: lcCopy,
					}
					reply := &AppendEntriesReply{}
					ok := rf.peers[p].Call("Raft.AppendEntries", args, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.processAppendEntriesReply(p, rpcTerm, pi, ent, reply)
				}(peer, prevI, prevT, lc, term, entries)
			}
			rf.mu.Unlock()
			time.Sleep(heartbeatInterval)
		} else {
			rf.tickFollower()
			time.Sleep(electionPollInterval)
		}
	}
}

// BackgroundApplyRoutine ships committed log entries to the service/tester on applyCh.
func (rf *Raft) BackgroundApplyRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.lastApplied++
		idx := rf.lastApplied
		offset := idx - rf.lastIncludedIndex - 1
		cmd := rf.log[offset].Command
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{Index: idx, Command: cmd}
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) != 0
}

// the service or tester calls Kill() when a Raft instance won't
// be needed again.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.applyCh = applyCh

	rf.serverState = follower
	rf.votedFor = -1
	rf.log = []LogEntry{}

	n := len(peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.BackgroundApplyRoutine()

	return rf
}
