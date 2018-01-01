package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	seq        int
	mu         sync.Mutex
	id         int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.seq = 1
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	leader := ck.lastLeader
	args := GetArgs{Key: key, Seq: ck.seq, ClientId: ck.id}
	ck.seq += 1
	ck.mu.Unlock()
	var reply GetReply

	value := ""
	for {
		ok := ck.servers[leader].Call("RaftKV.Get", &args, &reply)
		if ok && reply.Err == OK {
			value = reply.Value
			break
		} else if ok && reply.Err == ErrNoKey {
			break
		} else {
			DPrintf("Wrong leader %d", leader)
			leader = (leader + 1) % len(ck.servers)
		}
	}
	ck.mu.Lock()
	ck.lastLeader = leader
	ck.mu.Unlock()
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      ck.seq,
		ClientId: ck.id,
	}
	ck.seq += 1
	leader := ck.lastLeader
	ck.mu.Unlock()

	var reply PutAppendReply
	for {
		ok := ck.servers[leader].Call("RaftKV.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			break
		} else {
			DPrintf("Wrong leader %d", leader)
			leader = (leader + 1) % len(ck.servers)
		}
	}
	ck.lastLeader = leader
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
