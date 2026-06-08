package shardmaster

import (
	"crypto/x509"
	"encoding/gob"
	"sync"
	"weak"

	"time"

	"6.824/labrpc"
	"6.824/raft"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	numGroups int
	lastApplied map[int]int
	lastIncludedIndex int // last index applied to the sm
	results map[int]chan Op

	configs []Config // indexed by config num
}

const applyWaitTimeout = 500 * time.Millisecond


type Op struct {
	// Your data here.
	Type  string
	ReqId int
	Me    int64
	Servers map[int][]string // Join
    GIDs    []int           // Leave
    Shard   int             // Move
    GID     int             // Move
}

func (c Config) Copy() Config {
	nc := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range c.Groups {
		s := make([]string, len(servers))
		copy(s, servers)
		nc.Groups[gid] = s
	}
	return nc
}

func (sm *ShardMaster) isSameOp(op1 Op, op2 Op) bool {
	res := op1.Type == op2.Type && op1.ReqId == op2.ReqId && op1.Me == op2.Me
	return res
}

func (sm *ShardMaster) Rebalance(newConfig *Config) {
	//TODO, rebalance shards after every op

}


func (sm *ShardMaster) handleJoin(op *Op) Config {
	//Creates new config with groups from op.Servers
	prevConfig := sm.configs[len(sm.configs)-1]
	newConfig := prevConfig.Copy()

	for gid, server := range op.Servers {

		_, ok := newConfig.Groups[gid]
		
		if !ok {
			newConfig.Num += 1
		}
		newConfig.Groups[gid] = server
	}
	
	return newConfig

}

//TODO
func (sm *ShardMaster) handleLeave(op *Op) Config {
	prevConfig := sm.configs[len(sm.configs)-1]
	newConfig := prevConfig.Copy()

	for _, id := range op.GIDs {
		delete(newConfig.Groups, id)
	}
	return newConfig
}

func (sm *ShardMaster) getSmallestShardInGroup(config *Config, gid int) int {
	smallest := NShards + 1

	for i := range NShards {
		if config.Shards[i] == gid && i < smallest {
			smallest = i
		}
	}

	return smallest


}

func (sm *ShardMaster) handleMove(op *Op) Config {
	prevConfig := sm.configs[len(sm.configs)-1]
	newConfig := prevConfig.Copy()

	currShard := op.Shard
	currGroup := newConfig.Shards[currShard]

	swapGroup := op.GID
	swapShard := sm.getSmallestShardInGroup(&newConfig, swapGroup)

	newConfig.Shards[currShard] = swapGroup
	newConfig.Shards[swapShard] = currGroup
	return newConfig

}

func (sm *ShardMaster) handleQuery(op *Op) {
}

func (sm *ShardMaster) ApplyRoutine() {
	for {
		msg := <-sm.applyCh
		
		op := msg.Command.(Op)
		idx := msg.Index

		sm.mu.Lock()
		if op.Type == "Join" {
			newConfig := sm.handleJoin(&op)
			sm.Rebalance(&newConfig)
			sm.configs = append(sm.configs, newConfig)
		} else if op.Type == "Leave" {
			newConfig := sm.handleLeave(&op)
			sm.Rebalance(&newConfig)
			sm.configs = append(sm.configs, newConfig)
		} else if op.Type == "Move" {
			newConfig := sm.handleMove(&op)
			sm.configs = append(sm.configs, newConfig)
		} else if op.Type == "Query" {
			sm.handleQuery(&op)
		}
	
		ch, notify := sm.results[idx]
		sm.mu.Unlock()

		if notify {
			select {
			case ch <- op:
			default:
			}
		}

	}

}

func (kv *ShardMaster) waitAppliedOrTimeout(op Op) (bool, Op) {

	idx, _, leader := kv.rf.Start(op)

	if !leader {
		return false, op
	}

	ch := make(chan Op, 1)
	kv.mu.Lock()
	kv.results[idx] = ch
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		kv.mu.Lock()
		delete(kv.results, idx)
		kv.mu.Unlock()
		return kv.isSameOp(op, appliedOp), appliedOp
	case <-time.After(applyWaitTimeout):
		kv.mu.Lock()
		delete(kv.results, idx)
		kv.mu.Unlock()
		return false, op
	}

}



func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	reqId := args.Id
	op := Op{
		Type:"Join",
		ReqId: int(reqId),
		Servers: args.Servers,
	}

	success, _ := sm.waitAppliedOrTimeout(op)

	if !success {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	reqId := args.Id
	op := Op{
		Type:"Leave",
		ReqId: int(reqId),
		GIDs: args.GIDs,
	}

	success, _ := sm.waitAppliedOrTimeout(op)

	if !success {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	reqId := args.Id
	op := Op{
		Type:"Move",
		ReqId: int(reqId),
		Shard: args.Shard,
		GID:  args.GID,
	}

	success, _ := sm.waitAppliedOrTimeout(op)

	if !success {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// TODO
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.numGroups = 0
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.	

	return sm
}
