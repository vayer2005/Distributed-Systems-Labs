package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

// applyWaitTimeout bounds how long Get/PutAppend wait for Raft apply; after this
// we drop waiters so the clerk can retry another server (e.g. partitioned leader).
const applyWaitTimeout = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// opWaitKey identifies who submitted an op; Id alone is not unique across peers.
type opWaitKey struct {
	Me int64
	Id int
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	ReqId int
	Me    int64
	Key   string
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	results     map[int]chan Op
	store       map[string]string // key value state
	lastApplied map[int]int
	lastIncludedIndex int // last index applied to the sm
}
type SnapshotData struct {
	Store       map[string]string
	LastApplied map[int]int
	LastIncludedIndex int
}

// Handle put or append. Caller must hold kv.mu.
func (kv *RaftKV) handlePutAppend(op *Op) {
	if op.Type == "Put" {
		kv.store[op.Key] = op.Value
	} else if op.Type == "Append" {
		kv.store[op.Key] += op.Value
	}
}

func (kv *RaftKV) handleGet(op *Op) {
	val, ok := kv.store[op.Key]
	if ok {
		op.Value = val
	}
}

func (kv *RaftKV) sendSnapshot(raftIndex int) {
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.lastApplied)
	e.Encode(kv.lastIncludedIndex)
	kv.mu.Unlock()
	kv.rf.Snapshot(raftIndex, w.Bytes())
}

func (kv *RaftKV) ApplyRoutine() {
	for {
		msg := <-kv.applyCh
		if msg.UseSnapshot {
			data := msg.Snapshot
			r := bytes.NewBuffer(data)
			d := gob.NewDecoder(r)
			var store map[string]string
			var lastApplied map[int]int
			var lastIncludedIndex int
			if d.Decode(&store) != nil || d.Decode(&lastApplied) != nil || d.Decode(&lastIncludedIndex) != nil {
				continue
			}

			kv.mu.Lock()
			if lastIncludedIndex > kv.lastIncludedIndex {
				kv.lastIncludedIndex = lastIncludedIndex
				kv.store = store
				kv.lastApplied = lastApplied
			}
			kv.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		idx := msg.Index

		kv.mu.Lock()
		ch, ok1 := kv.results[idx]

		if op.Type == "GET" {
			kv.handleGet(&op)
		} else {
			lastIdx, ok2 := kv.lastApplied[int(op.Me)]
			if !ok2 || lastIdx < op.ReqId {
				kv.lastApplied[int(op.Me)] = op.ReqId
				kv.handlePutAppend(&op)
				kv.lastIncludedIndex = idx
			}
		}

		if !ok1 {
			ch = make(chan Op, 1)
			kv.results[idx] = ch
		}
		kv.mu.Unlock()
		
		if kv.maxraftstate > 0 && kv.rf.RaftSize() >= kv.maxraftstate/kv.rf.NumPeers() {
			kv.sendSnapshot(idx)
		}

		ch <- op

	}

}

func (kv *RaftKV) isSameOp(op1 Op, op2 Op) bool {
	res := op1.Type == op2.Type && op1.Key == op2.Key && op1.ReqId == op2.ReqId && op1.Me == op2.Me
	return res
}

// waitAppliedOrTimeout waits until ApplyRoutine closes ch, or times out and removes
// all waiters for waitKey so RPC handlers do not block forever.
func (kv *RaftKV) waitAppliedOrTimeout(op Op) (bool, Op) {

	idx, _, leader := kv.rf.Start(op)

	if !leader {
		return false, op
	}

	kv.mu.Lock()

	ch, ok := kv.results[idx]
	if !ok {
		ch = make(chan Op, 1)
		kv.results[idx] = ch
	}
	kv.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		return kv.isSameOp(op, appliedOp), appliedOp
	case <-time.After(600 * time.Millisecond):
		return false, op
	}

}

func (kv *RaftKV) getRaftLeader() (wrongLeader bool, err Err) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return true, ErrWrongLeader
	}
	return false, OK
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//TODO wait for apply, once applied return

	op := Op{Key: args.Key, Value: "", Type: "GET", ReqId: args.Id, Me: args.Me}

	success, appledOp := kv.waitAppliedOrTimeout(op)

	if !success {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
	reply.Value = appledOp.Value

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key: args.Key, Value: args.Value, Type: args.Op, ReqId: args.Id, Me: args.Me}

	success, _ := kv.waitAppliedOrTimeout(op)

	if !success {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired
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
	//kv.lastApplied = make(map[int64]int)
	kv.applyCh = make(chan raft.ApplyMsg, 2048)
	kv.results = make(map[int]chan Op)
	kv.lastIncludedIndex = 0
	kv.lastApplied = make(map[int]int)
	go kv.ApplyRoutine()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
