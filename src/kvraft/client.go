package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
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
	args := GetArgs{Key: key}
	var reply GetReply
	leader := ck.lastLeader
	value := ""
	for {
		DPrintf("%v to %d", args, leader)
		ok := ck.servers[leader].Call("RaftKV.Get", &args, &reply)
		DPrintf("%b %s", reply.WrongLeader, reply.Err)
		if ok && reply.Err == OK {
			value = reply.Value
			break
		} else if ok && reply.Err == ErrNoKey {
			break
		} else {
			leader = (leader + 1) % len(ck.servers)
		}
	}
	ck.lastLeader = leader
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
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	var reply PutAppendReply
	leader := ck.lastLeader
	for {
		DPrintf("%v to %d", args, leader)
		ok := ck.servers[leader].Call("RaftKV.PutAppend", &args, &reply)
		DPrintf("%v", reply)
		if ok && reply.Err == OK {
			break
		} else {
			leader = (leader + 1) % len(ck.servers)
		}
	}
	ck.lastLeader = leader
	DPrintf("Succeed")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
