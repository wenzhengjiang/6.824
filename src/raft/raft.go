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
	currentTerm int
	votedFor    int
	log         []LogEntry

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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.currentTerm
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
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
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
	rf.p("reply requestVote from %d", args.CandidateId)
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs) RequestVoteReply {
	var voteGranted bool
	lastTerm := rf.log[len(rf.log)-1].Term
	if args.Term < rf.currentTerm {
		voteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= len(rf.log)-1)) { // Election restriction

		voteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
	} else {
		// Don't vote for two candiate at the same time
		voteGranted = false
	}

	return RequestVoteReply{VoteGranted: voteGranted, Term: rf.currentTerm, Peer: rf.me}
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
	Term    int
	Success bool
	PeerId  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.p("receive appendEntries from %d", args.LeaderId)
	rf.appendEntriesArgsCh <- args
	*reply = <-rf.appendEntriesReplyCh
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs) AppendEntriesReply {
	var success bool
	if args.Term < rf.currentTerm {
		success = false
	} else if len(args.Entries) == 0 { // Heartbeat
		success = true
	} else if args.PrevLogIndex >= len(rf.log) || // Log inconsistency
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

		success = false
	} else { // Log replication
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}

		if args.LeaderCommit > len(rf.log)-1 {
			rf.p("LeaderCommit not exist in local logs ???? ")
		}
		success = true
	}
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
	}
	return AppendEntriesReply{Term: rf.currentTerm, Success: success, PeerId: rf.me}
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
		rf.p("failed to send requestVote to %d", server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.p("send appendEntries to %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		rf.p("failed to send appendEntries to %d", server)
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
	rf.clientRequestCh <- command
	index := <-rf.clientReplyCh

	return index, rf.log[index].Term, true
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
	rf.p("runFollower")
	timer := rf.getElectionTimer()
	for {
		select {
		case <-timer.C:
			rf.p("Election timeout")
			// Timeout then become candidate
			go rf.runCandidate()
			return
		case args := <-rf.appendEntriesArgsCh:
			rf.appendEntriesReplyCh <- rf.handleAppendEntries(args)
			timer = rf.getElectionTimer()
		case args := <-rf.requestVoteArgsCh:
			rf.requestVoteReplyCh <- rf.handleRequestVote(args)
			timer = rf.getElectionTimer()
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
			args := RequestVoteArgs{Term: term, CandidateId: id}
			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}
			voteCh <- reply
		}(rf.currentTerm, rf.me, i)
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
	rf.votedFor = rf.me

	rf.currentTerm += 1
	voteCount := 1
	voteCh := rf.requestVotes()
	rf.p("runCandidate")

	timer := rf.getElectionTimer()
	majority := len(rf.peers)/2 + 1
	for {
		// The new leader within five seconds
		// Hearbeats is sent per 100ms
		// we choose the election timeout to be 800ms ~ 1s. 5 elections should be enough ?
		select {
		case <-timer.C:
			rf.p("Election timeout")
			voteCount = 0
			timer = rf.getElectionTimer()
			rf.currentTerm += 1
			voteCount = 1
			voteCh = rf.requestVotes()
		case args := <-rf.appendEntriesArgsCh:
			reply := rf.handleAppendEntries(args)
			rf.appendEntriesReplyCh <- reply
			if reply.Success {
				go rf.runFollower()
			}
		case args := <-rf.requestVoteArgsCh:
			reply := rf.handleRequestVote(args)
			rf.requestVoteReplyCh <- reply
			if reply.VoteGranted {
				go rf.runFollower()
				return
			}
		case reply := <-voteCh:
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = reply.Peer
				go rf.runFollower()
				return
			}
			if reply.VoteGranted && rf.currentTerm == reply.Term {
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

func (rf *Raft) sendHeartBeats() chan AppendEntriesReply {
	replyCh := make(chan AppendEntriesReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, term int, me int) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &AppendEntriesArgs{Term: term, LeaderId: me}, &reply) {
				replyCh <- reply
			}
		}(i, rf.currentTerm, rf.me)
	}
	return replyCh
}

func (rf *Raft) p(format string, a ...interface{}) (n int, err error) {
	if rf.debug > 0 {
		state := "follower"
		if rf.isLeader {
			state = "leader"
		} else if rf.votedFor == rf.me {
			state = "candidate"
		}
		log.Printf("[%d][%s][%d]: %s", rf.me, state, rf.currentTerm, fmt.Sprintf(format, a...))
	}
	return
}

func (rf *Raft) replicateLogs(server int, replyCh chan AppendEntriesReply, quit chan bool) {
	for {
		select {
		case <-quit:
			return
		default:
		}
		index := len(rf.log) - 1
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      rf.log[rf.nextIndex[server] : index+1],
			LeaderCommit: rf.commitIndex,
		}
		var reply AppendEntriesReply
		rf.sendAppendEntries(server, &args, &reply)
		if reply.Success {
			rf.nextIndex[server] = index + 1
			rf.matchIndex[server] = index
		} else if reply.Term <= rf.currentTerm {
			rf.nextIndex[server] -= 1
		} else {
			replyCh <- reply
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex(quit chan bool) {
	for {
		majority := len(rf.peers)/2 + 1
		select {
		case <-quit:
			return
		default:
		}
		index := rf.commitIndex + 1
		count := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= index {
				count += 1
			}
		}
		if count >= majority {
			rf.commitIndex = index
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) runLeader() {
	rf.isLeader = true
	rf.p("runLeader")
	heartBeatReplyCh := rf.sendHeartBeats()
	ticker := time.NewTicker(100 * time.Millisecond)
	appendEntriesReplyCh := make(chan AppendEntriesReply)
	var quitChs []chan bool
	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
			quit := make(chan bool, 1)
			quitChs = append(quitChs, quit)
			go rf.replicateLogs(i, appendEntriesReplyCh, quit)
		}
	}
	quit := make(chan bool, 1)
	quitChs = append(quitChs, quit)
	go rf.updateCommitIndex(quit)

	for {
		select {
		case <-ticker.C:
			heartBeatReplyCh = rf.sendHeartBeats()
		case args := <-rf.appendEntriesArgsCh:
			reply := rf.handleAppendEntries(args)
			rf.appendEntriesReplyCh <- reply
			if reply.Success {
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
		case reply := <-heartBeatReplyCh:
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = reply.PeerId
				go rf.runFollower()
				return
			}
		case reply := <-appendEntriesReplyCh:
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = reply.PeerId
				go rf.runFollower()
				return
			}
		case command := <-rf.clientRequestCh:
			entry := LogEntry{Command: command, Term: rf.currentTerm}
			rf.log = append(rf.log, entry)
			index := len(rf.log) - 1
			rf.clientReplyCh <- index
		}
	}
}

func (rf *Raft) applyStateMachine(applyCh chan ApplyMsg) {
	for {
		for index := rf.lastApplied + 1; index <= rf.commitIndex; index += 1 {
			applyCh <- ApplyMsg{
				Index:   index,
				Command: rf.log[index].Command,
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
		currentTerm:          0,
		votedFor:             -1,
		log:                  []LogEntry{LogEntry{nil, 0}},
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
	go rf.applyStateMachine(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
