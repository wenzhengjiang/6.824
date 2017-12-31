package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Op    string
	Key   string
	value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	executions map[int]chan bool
	data       map[string]string
}

func (kv *RaftKV) applyReceiver() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		kv.executions[msg.Index] <- true
		kv.mu.Unlock()
	}
}
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{Op: "Get", Key: args.Key})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	resultCh := make(chan bool)
	kv.mu.Lock()
	if ch, ok := kv.executions[index]; ok {
		close(ch)
	}
	kv.executions[index] = resultCh
	kv.mu.Unlock()

	if !<-resultCh {
		// Log is overwritten
		reply.Err = ErrFailed
		return
	}

	kv.mu.Lock()
	value, ok := kv.data[args.Key]
	kv.mu.Unlock()
	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = value
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{args.Op, args.Key, args.Value})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	resultCh := make(chan bool)
	kv.mu.Lock()
	if ch, ok := kv.executions[index]; ok {
		close(ch)
	}
	kv.executions[index] = resultCh
	kv.mu.Unlock()

	if !<-resultCh {
		// Log is overwritten
		reply.Err = ErrFailed
		return
	}

	kv.mu.Lock()
	if args.Op == "Put" {
		kv.data[args.Key] = args.Value
	} else {
		kv.data[args.Key] += args.Value
	}
	kv.mu.Unlock()
	reply.Err = OK
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.executions = make(map[int]chan bool)

	go kv.applyReceiver()

	// You may need initialization code here.

	return kv
}
