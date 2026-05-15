package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	clientId int64 // random per clerk; stable for lifetime of this Clerk
	id       int   // id of operation
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
	ck.leader = 0
	ck.clientId = nrand()

	return ck
}

func (ck *Clerk) getLeader() int {
	return ck.leader
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.id += 1
	args := GetArgs{Key: key, Id: ck.id, Me: ck.clientId}

	for {
		reply := GetReply{}
		leader := ck.getLeader()

		ok := ck.servers[leader].Call("RaftKV.Get", &args, &reply)
		if !ok || reply.WrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if !ok || reply.Err == ErrApplyTimeout {
			continue
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.id += 1
	args := PutAppendArgs{Key: key, Value: value, Op: op, Id: ck.id, Me: ck.clientId}

	for {
		reply := PutAppendReply{}
		leader := ck.getLeader()
		ok := ck.servers[leader].Call("RaftKV.PutAppend", &args, &reply)
		if !ok || reply.WrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if !ok || reply.Err == ErrApplyTimeout {
			continue
		}
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
