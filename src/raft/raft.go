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
	isLeader    bool
	currentTerm int
	votedFor    int

	appendEntriesArgsChan  chan *AppendEntriesArgs
	appendEntriesReplyChan chan AppendEntriesReply
	requestVoteArgsChan    chan *RequestVoteArgs
	requestVoteReplyChan   chan RequestVoteReply

	debug int
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
	Term        int
	CandidateId int
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
	rf.log("receive requestVote from %d", args.CandidateId)
	rf.requestVoteArgsChan <- args
	*reply = <-rf.requestVoteReplyChan
	rf.log("reply requestVote from %d", args.CandidateId)
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs) RequestVoteReply {
	var voteGranted bool
	if args.Term > rf.currentTerm {
		voteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
	} else if args.Term < rf.currentTerm {
		voteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		voteGranted = true
		rf.votedFor = args.CandidateId
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
	Term     int
	LeaderId int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.log("receive appendEntries from %d", args.LeaderId)
	rf.appendEntriesArgsChan <- args
	*reply = <-rf.appendEntriesReplyChan
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs) AppendEntriesReply {
	var success bool
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		success = true
	} else if args.Term < rf.currentTerm {
		success = false
	}
	return AppendEntriesReply{Term: rf.currentTerm, Success: success}
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
	rf.log("send requestVote to %d", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		rf.log("failed to send requestVote to %d", server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.log("send appendEntries to %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		rf.log("failed to send appendEntries to %d", server)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	rf.log("runFollower")
	timer := rf.getElectionTimer()
	for {
		select {
		case <-timer.C:
			rf.log("Election timeout")
			// Timeout then become candidate
			go rf.runCandidate()
			return
		case args := <-rf.appendEntriesArgsChan:
			rf.appendEntriesReplyChan <- rf.handleAppendEntries(args)
			timer = rf.getElectionTimer()
		case args := <-rf.requestVoteArgsChan:
			rf.requestVoteReplyChan <- rf.handleRequestVote(args)
			timer = rf.getElectionTimer()
		default:
		}
	}
}

func (rf *Raft) requestVotes() chan RequestVoteReply {
	voteChan := make(chan RequestVoteReply)

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
			voteChan <- reply
		}(rf.currentTerm, rf.me, i)
	}
	return voteChan
}

//
// Send vote requests to all peers.
// * If timeout, start another vote collection
// * If get `yes` from majority, become leader
// * If get legit heartbeat, become follower
//
func (rf *Raft) runCandidate() {
	rf.isLeader = false
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.log("runCandidate")
	voteChan := rf.requestVotes()

	timer := rf.getElectionTimer()
	voteCount := 1
	majority := len(rf.peers)/2 + 1
	for {
		// The new leader within five seconds
		// Hearbeats is sent per 100ms
		// we choose the election timeout to be 800ms ~ 1s. 5 elections should be enough ?
		select {
		case <-timer.C:
			rf.log("Election timeout")
			voteCount = 0
			timer = rf.getElectionTimer()
			rf.currentTerm += 1
			voteChan = rf.requestVotes()
		case args := <-rf.appendEntriesArgsChan:
			reply := rf.handleAppendEntries(args)
			rf.appendEntriesReplyChan <- reply
			if reply.Success {
				go rf.runFollower()
			}
		case args := <-rf.requestVoteArgsChan:
			reply := rf.handleRequestVote(args)
			rf.requestVoteReplyChan <- reply
			if reply.VoteGranted {
				go rf.runFollower()
				return
			}
		case reply := <-voteChan:
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = reply.Peer
				go rf.runFollower()
				return
			}
			if reply.VoteGranted && rf.currentTerm == reply.Term {
				voteCount += 1
				rf.log("receive vote from %d, %d votes in total", reply.Peer, voteCount)
			}
			if voteCount >= majority {
				go rf.runLeader()
				return
			}
		}
	}
}

func (rf *Raft) sendHeartBeats() bool {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		var reply AppendEntriesReply
		if rf.sendAppendEntries(i, &AppendEntriesArgs{rf.currentTerm, rf.me}, &reply) {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = i
				return false
			}
		}
	}
	return true
}

func (rf *Raft) log(format string, a ...interface{}) (n int, err error) {
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
func (rf *Raft) runLeader() {
	rf.isLeader = true
	rf.log("runLeader")
	if !rf.sendHeartBeats() {
		go rf.runFollower()
		return
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if !rf.sendHeartBeats() {
				go rf.runFollower()
				return
			}
		case args := <-rf.appendEntriesArgsChan:
			reply := rf.handleAppendEntries(args)
			rf.appendEntriesReplyChan <- reply
			if reply.Success {
				go rf.runFollower()
				return
			}
		case args := <-rf.requestVoteArgsChan:
			reply := rf.handleRequestVote(args)
			rf.requestVoteReplyChan <- reply
			if reply.VoteGranted {
				go rf.runFollower()
				return
			}
		default:
		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.appendEntriesArgsChan = make(chan *AppendEntriesArgs)
	rf.appendEntriesReplyChan = make(chan AppendEntriesReply)
	rf.requestVoteArgsChan = make(chan *RequestVoteArgs)
	rf.requestVoteReplyChan = make(chan RequestVoteReply)
	rf.debug = Debug

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().Unix())
	go rf.runFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
