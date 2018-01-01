package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Op       string
	Key      string
	Value    string
	ClientId int64
	Seq      int
}

type RaftKV struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	executions  map[int]chan Op
	data        map[string]string
	appliedSeqs map[int64]int
}

func (kv *RaftKV) p(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[%d] %s", kv.me, fmt.Sprintf(format, a...))
	}
	return
}
func (kv *RaftKV) applyReceiver() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		op := msg.Command.(Op)
		if kv.appliedSeqs[op.ClientId] != op.Seq {
			kv.p("Appling %d %v", msg.Index, op)
			kv.appliedSeqs[op.ClientId] = op.Seq
			switch op.Op {
			case Put:
				kv.data[op.Key] = op.Value
			case Append:
				kv.data[op.Key] += op.Value
			case Get:
			default:
				log.Fatal("Invalid op - %s", op.Op)
			}
		} else {
			kv.p("Duplicated operation %d %v", msg.Index, op)
		}
		if ch, ok := kv.executions[msg.Index]; ok {
			ch <- op
		}
		kv.p("Applied successfully")
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	setReply := func() {
		kv.mu.RLock()
		value, ok := kv.data[args.Key]
		kv.mu.RUnlock()
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = value
		}
	}
	defer setReply()
	kv.mu.RLock()
	if kv.appliedSeqs[args.ClientId] == args.Seq {
		kv.mu.RUnlock()
		setReply()
		return
	}
	kv.mu.RUnlock()

	op := Op{Op: Get, Key: args.Key, ClientId: args.ClientId, Seq: args.Seq}
	index, startTerm, _ := kv.rf.Start(op)

	kv.mu.Lock()
	if ch, ok := kv.executions[index]; ok {
		close(ch)
	}
	resultCh := make(chan Op)
	kv.executions[index] = resultCh
	kv.mu.Unlock()

	if <-resultCh != op {
		// Log is overwritten
		reply.Err = ErrFailed
		return
	}
	if endTerm, _ := kv.rf.GetState(); endTerm != startTerm {
		log.Fatal("startTerm:%d, endTerm:%d", startTerm, endTerm)
	}

	kv.mu.Lock()
	close(kv.executions[index])
	delete(kv.executions, index)
	kv.mu.Unlock()

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.RLock()
	if kv.appliedSeqs[args.ClientId] == args.Seq {
		kv.mu.RUnlock()
		reply.Err = OK
		return
	}
	kv.mu.RUnlock()

	op := Op{Op: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, Seq: args.Seq}
	index, startTerm, _ := kv.rf.Start(op)

	resultCh := make(chan Op)
	kv.mu.Lock()
	if ch, ok := kv.executions[index]; ok {
		close(ch)
	}
	kv.executions[index] = resultCh
	kv.mu.Unlock()

	if <-resultCh != op {
		// Log is overwritten
		reply.Err = ErrFailed
		return
	}
	if endTerm, _ := kv.rf.GetState(); endTerm != startTerm {
		log.Fatal("startTerm:%d, endTerm:%d", startTerm, endTerm)
	}

	kv.mu.Lock()
	close(kv.executions[index])
	delete(kv.executions, index)
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
	kv.executions = make(map[int]chan Op)
	kv.appliedSeqs = make(map[int64]int)

	go kv.applyReceiver()

	return kv
}
