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

// opWaitKey identifies who submitted an op; Id alone is not unique across peers.
type opWaitKey struct {
	Me int
	Id int
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int
	Me  int
	Req any
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	waiters map[opWaitKey]chan struct{}
	id      int               // atomically increasing op id
	store   map[string]string // key value state
}

// Handle put or append. Caller must hold kv.mu.
func (kv *RaftKV) handlePutAppend(pa *PutAppendArgs) {
	if pa.Op == "Put" {
		kv.store[pa.Key] = pa.Value
	} else if pa.Op == "Append" {
		kv.store[pa.Key] += pa.Value
	}
}

func (kv *RaftKV) ApplyRoutine() {
	for {
		msg := <-kv.applyCh

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()
		if pa, ok := op.Req.(*PutAppendArgs); ok {
			kv.handlePutAppend(pa)
		}
		wkey := opWaitKey{Me: op.Me, Id: op.Id}
		if ch, found := kv.waiters[wkey]; found {
			delete(kv.waiters, wkey)
			close(ch)
		}
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//TODO: Append the current get to log, wait for apply, once applied return
	kv.mu.Lock()
	id := kv.id
	me := kv.me
	kv.id++
	waitKey := opWaitKey{Me: me, Id: id}
	ch := make(chan struct{})
	kv.waiters[waitKey] = ch
	kv.mu.Unlock()
	op := Op{Me: me, Id: id, Req: args}

	_, _, leader := kv.rf.Start(op)

	if !leader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		close(ch)
		delete(kv.waiters, waitKey)
		kv.mu.Unlock()
		return
	}

	<-ch
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.store[args.Key]
	reply.WrongLeader = false
	if !ok {
		reply.Value = ""
		reply.Err = ErrNoKey
	} else {
		reply.Value = val
		reply.Err = OK
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	id := kv.id
	me := kv.me
	kv.id++
	waitKey := opWaitKey{Me: me, Id: id}
	ch := make(chan struct{})
	kv.waiters[waitKey] = ch
	kv.mu.Unlock()
	op := Op{Me: me, Id: id, Req: args}

	_, _, leader := kv.rf.Start(op)

	if !leader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		close(ch)
		delete(kv.waiters, waitKey)
		kv.mu.Unlock()
		return
	}

	<-ch
	reply.WrongLeader = false
	reply.Err = OK
}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.store = make(map[string]string)
	kv.waiters = make(map[opWaitKey]chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.ApplyRoutine()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
