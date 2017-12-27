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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	appendEntriesArgsCh  chan *AppendEntriesArgs
	appendEntriesReplyCh chan AppendEntriesReply
	requestVoteArgsCh    chan *RequestVoteArgs
	requestVoteReplyCh   chan RequestVoteReply
	clientRequestCh      chan interface{}
	clientReplyCh        chan int

	applyCh  chan ApplyMsg
	isLeader bool
	debug    int
	replyCh  chan AppendEntriesReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.CurrentTerm
	var isleader bool = rf.isLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Peer        int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.p("receive requestVote from %d", args.CandidateId)
	rf.requestVoteArgsCh <- args
	rf.p("handling requestVote from %d", args.CandidateId)
	*reply = <-rf.requestVoteReplyCh
	rf.p("reply requestVote %v from %d", reply, args.CandidateId)
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs) RequestVoteReply {
	var voteGranted bool
	lastTerm := rf.Log[len(rf.Log)-1].Term
	if args.Term < rf.CurrentTerm {
		voteGranted = false
	} else if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId || rf.CurrentTerm < args.Term) &&
		(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= len(rf.Log)-1)) { // Election restriction

		voteGranted = true
		rf.VotedFor = args.CandidateId
		rf.CurrentTerm = args.Term
	} else {
		// Don't vote for two candiate at the same time
		voteGranted = false
	}

	// Got request from a condidate with higher term but stale log
	if !voteGranted && args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}
	rf.persist()
	return RequestVoteReply{VoteGranted: voteGranted, Term: rf.CurrentTerm, Peer: rf.me}
}

//
// example Appendentries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term              int
	Success           bool
	PeerId            int
	ConflictTerm      int
	ConflictTermIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.p("receive appendEntries %v from %d", args, args.LeaderId)
	rf.appendEntriesArgsCh <- args
	*reply = <-rf.appendEntriesReplyCh
	//rf.p("reply appendEntries %v to %d", reply, args.LeaderId)
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs) AppendEntriesReply {
	var success bool
	var conflictTerm, conflictTermIndex int
	if args.Term < rf.CurrentTerm {
		success = false
	} else if args.PrevLogIndex >= len(rf.Log) || // Log inconsistency
		rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		index := args.PrevLogIndex
		if index >= len(rf.Log) {
			index = len(rf.Log) - 1
		}
		conflictTerm = rf.Log[index].Term
		for ; index > 0 && rf.Log[index].Term == conflictTerm; index -= 1 {
			conflictTermIndex = index
		}
		success = false
	} else { // Log replication
		// Find first inconsistent log entry
		logIndex := args.PrevLogIndex + 1
		entryIndex := 0
		for {
			if logIndex >= len(rf.Log) {
				break
			}
			if entryIndex >= len(args.Entries) {
				break
			}
			if rf.Log[logIndex].Term != args.Entries[entryIndex].Term {
				break
			}
			logIndex += 1
			entryIndex += 1
		}

		if entryIndex < len(args.Entries) { // If there are entries to append
			rf.Log = append(rf.Log[:logIndex], args.Entries[entryIndex:]...)
		}
		if args.LeaderCommit < len(rf.Log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.Log) - 1
		}

		if args.LeaderCommit > len(rf.Log)-1 {
			rf.p("LeaderCommit not exist in local logs ???? ")
		}
		success = true
	}
	if args.Term >= rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderId
	}

	rf.persist()
	return AppendEntriesReply{
		Term:              rf.CurrentTerm,
		Success:           success,
		PeerId:            rf.me,
		ConflictTerm:      conflictTerm,
		ConflictTermIndex: conflictTermIndex,
	}
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.p("send requestVote to %d", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//rf.p("failed to send requestVote to %d", server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.p("send appendEntries to %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		//rf.p("failed to send appendEntries to %d", server)
	}
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if !rf.isLeader {
		return 0, 0, false
	}
	rf.mu.Lock()
	rf.p("Start")
	entry := LogEntry{Command: command, Term: rf.CurrentTerm}
	rf.Log = append(rf.Log, entry)
	rf.persist()
	index := len(rf.Log) - 1
	rf.p("Start return %d", index)
	rf.mu.Unlock()
	return index, rf.Log[index].Term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.debug = 0
}

func (rf *Raft) getElectionTimer() *time.Timer {
	return time.NewTimer((time.Duration(rand.Intn(150) + 600)) * time.Millisecond)
}

//
// Check heartbeat periodically.
// If not received, increase term and become a candidate
//
func (rf *Raft) runFollower() {
	rf.isLeader = false
	rf.matchIndex = nil
	rf.nextIndex = nil
	rf.p("runFollower")
	timer := rf.getElectionTimer()

	for {
		select {
		case <-timer.C:
			rf.p("follower Election timeout")
			// Timeout then become candidate
			go rf.runCandidate()
			return
		case args := <-rf.appendEntriesArgsCh:
			rf.appendEntriesReplyCh <- rf.handleAppendEntries(args)
			if args.Term >= rf.CurrentTerm { // From current leader
				timer = rf.getElectionTimer()
			}
		case args := <-rf.requestVoteArgsCh:
			reply := rf.handleRequestVote(args)
			rf.requestVoteReplyCh <- reply
			if reply.VoteGranted { // Grant vote to candidate
				timer = rf.getElectionTimer()
			}
		}
	}
}

func (rf *Raft) requestVotes() chan RequestVoteReply {
	voteCh := make(chan RequestVoteReply)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(term int, id int, peer int) {
			var reply RequestVoteReply
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  id,
				LastLogIndex: len(rf.Log) - 1,
				LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
			}
			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}
			voteCh <- reply
		}(rf.CurrentTerm, rf.me, i)
	}
	return voteCh
}

//
// Send vote requests to all peers.
// * If timeout, start another vote collection
// * If get `yes` from majority, become leader
// * If get legit heartbeat, become follower
//
func (rf *Raft) runCandidate() {
	rf.isLeader = false
	rf.VotedFor = rf.me
	rf.CurrentTerm += 1
	rf.persist()

	rf.matchIndex = nil
	rf.nextIndex = nil
	voteCount := 1
	rf.p("runCandidate")
	voteCh := rf.requestVotes()

	timer := rf.getElectionTimer()
	majority := len(rf.peers)/2 + 1
	for {
		// The new leader within five seconds
		// Hearbeats is sent per 100ms
		// we choose the election timeout to be 800ms ~ 1s. 5 elections should be enough ?
		select {
		case <-timer.C:
			rf.p("candidate Election timeout")
			voteCount = 0
			timer = rf.getElectionTimer()
			rf.CurrentTerm += 1
			voteCount = 1
			voteCh = rf.requestVotes()
		case args := <-rf.appendEntriesArgsCh:
			reply := rf.handleAppendEntries(args)
			rf.appendEntriesReplyCh <- reply
			if reply.Success || rf.VotedFor != rf.me {
				go rf.runFollower()
				return
			}
		case args := <-rf.requestVoteArgsCh:
			reply := rf.handleRequestVote(args)
			rf.requestVoteReplyCh <- reply
			if reply.VoteGranted {
				go rf.runFollower()
				return
			}
		case reply := <-voteCh:
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = reply.Peer
				rf.persist()
				go rf.runFollower()
				return
			}
			if reply.VoteGranted && rf.CurrentTerm == reply.Term {
				voteCount += 1
				rf.p("receive vote from %d, %d votes in total", reply.Peer, voteCount)
			}
			if voteCount >= majority {
				go rf.runLeader()
				return
			}
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.replyCh <- reply
			}
		}(i, AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.Log) - 1,
			PrevLogTerm:  rf.Log[len(rf.Log)-1].Term,
			LeaderCommit: rf.commitIndex,
		})
	}
}

func (rf *Raft) p(format string, a ...interface{}) (n int, err error) {
	if rf.debug > 0 {
		state := "follower"
		if rf.isLeader {
			state = "leader"
		} else if rf.VotedFor == rf.me {
			state = "candidate"
		}
		log.Printf("[%d][%s][%d]: %s", rf.me, state, rf.CurrentTerm, fmt.Sprintf(format, a...))
	}
	return
}

func (rf *Raft) runLeader() {
	rf.isLeader = true
	rf.p("runLeader")
	rf.sendHeartBeats()
	ticker := time.NewTicker(100 * time.Millisecond)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.Log)
			rf.matchIndex[i] = 0
		}
	}

	for {
		select {
		case args := <-rf.appendEntriesArgsCh:
			reply := rf.handleAppendEntries(args)
			rf.appendEntriesReplyCh <- reply
			if reply.Success || rf.VotedFor != rf.me {
				go rf.runFollower()
				return
			}
		case args := <-rf.requestVoteArgsCh:
			reply := rf.handleRequestVote(args)
			rf.requestVoteReplyCh <- reply
			if reply.VoteGranted || rf.VotedFor == -1 {
				go rf.runFollower()
				return
			}
		case reply := <-rf.replyCh:
			rf.p("AppendEntires response %v", reply)
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = reply.PeerId
				rf.persist()
				go rf.runFollower()
				return
			}
		case <-ticker.C:
			rf.sendHeartBeats()
		}
	}
}

//
// Replicate logs to other servers
// Write: nextIndex, matchIndex
//
func (rf *Raft) replicater(server int) {
	for {
		lastIndex := len(rf.Log) - 1

		if rf.isLeader && lastIndex >= rf.nextIndex[server] {
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.Log[rf.nextIndex[server]-1].Term,
				Entries:      rf.Log[rf.nextIndex[server] : lastIndex+1],
				LeaderCommit: rf.commitIndex,
			}
			rf.p("replicate %v to %d", args, server)
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				continue
			}
			if reply.Success {
				rf.mu.Lock()
				rf.nextIndex[server] = lastIndex + 1
				rf.matchIndex[server] = lastIndex
				rf.p("Successfully replicate to %d", server)
				rf.commit()
				rf.mu.Unlock()
			} else if reply.Term <= rf.CurrentTerm {
				rf.mu.Lock()
				for rf.nextIndex[server] -= 1; rf.Log[rf.nextIndex[server]].Term > reply.ConflictTerm; rf.nextIndex[server] -= 1 {
				}
				if rf.Log[rf.nextIndex[server]].Term != reply.ConflictTerm {
					rf.nextIndex[server] = reply.ConflictTermIndex
				}
				rf.mu.Unlock()
				rf.p("Re-try replicate to %d@%d", server, rf.nextIndex[server])
				continue
			} else {
				rf.replyCh <- reply
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

//
// Commit logs replicated to majority nodes during current term
// Write: commitIndex
//
func (rf *Raft) commit() {
	majority := len(rf.peers)/2 + 1
	for N := len(rf.Log) - 1; N > rf.commitIndex; N -= 1 {
		count := 1
		for _, index := range rf.matchIndex {
			if index >= N {
				count += 1
			}
		}
		if count >= majority && rf.Log[N].Term == rf.CurrentTerm {
			rf.p("commit %d", N)
			rf.commitIndex = N
			break
		}
	}
}

//
// Apply committed commands periodically
// Write: lastApplied
//
func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for {
		index := rf.lastApplied + 1
		if index <= rf.commitIndex {
			rf.p("apply %d", index)
			applyCh <- ApplyMsg{
				Index:   index,
				Command: rf.Log[index].Command,
			}
			rf.lastApplied += 1
		}
		time.Sleep(10 * time.Millisecond)
	}
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:                peers,
		persister:            persister,
		me:                   me,
		CurrentTerm:          0,
		VotedFor:             -1,
		Log:                  []LogEntry{LogEntry{nil, 0}},
		commitIndex:          0,
		lastApplied:          0,
		appendEntriesArgsCh:  make(chan *AppendEntriesArgs),
		appendEntriesReplyCh: make(chan AppendEntriesReply),
		requestVoteArgsCh:    make(chan *RequestVoteArgs),
		requestVoteReplyCh:   make(chan RequestVoteReply),
		debug:                Debug,
	}

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().Unix())
	go rf.runFollower()
	go rf.applier(applyCh)
	for server := range rf.peers {
		if server != me {
			go rf.replicater(server)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
