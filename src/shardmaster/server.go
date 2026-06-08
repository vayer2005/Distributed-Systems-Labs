package shardmaster

import (
	"encoding/gob"
	"sync"
	"sort"

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
	Num     int             // Query
	config 	Config
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

func (sm *ShardMaster) Rebalance(config *Config) {
	if len(config.Groups) == 0 {
		for s := 0; s < NShards; s++ {
			config.Shards[s] = 0
		}
		return
	}
	// 1. Sorted GIDs
	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	n := len(gids)
	avg := NShards / n
	rem := NShards % n
	target := func(i int) int {
		if i < rem {
			return avg + 1
		}
		return avg
	}
	// 2. Count valid assignments; collect bad shards
	count := make(map[int]int)
	for _, gid := range gids {
		count[gid] = 0
	}
	var pool []int // shard indices that need a new owner
	for s := 0; s < NShards; s++ {
		gid := config.Shards[s]
		if _, ok := config.Groups[gid]; !ok {
			pool = append(pool, s)
		} else {
			count[gid]++
		}
	}
	// 3. Groups with too many shards donate (deterministic: high shard index first)
	for i, gid := range gids {
		for count[gid] > target(i) {
			count[gid]--
			for s := NShards - 1; s >= 0; s-- {
				if config.Shards[s] == gid {
					config.Shards[s] = 0
					pool = append(pool, s)
					break
				}
			}
		}
	}
	// 4. Groups with too few shards receive
	var need []int
	for i, gid := range gids {
		for count[gid] < target(i) {
			need = append(need, gid)
			count[gid]++
		}
	}
	// 5. Assign pooled shards to needy groups
	sort.Ints(pool)
	for i, s := range pool {
		config.Shards[s] = need[i]
	}
	

}


func (sm *ShardMaster) handleJoin(op *Op) Config {
	//Creates new config with groups from op.Servers
	prevConfig := sm.configs[len(sm.configs)-1]
	newConfig := prevConfig.Copy()

	for gid, server := range op.Servers {		
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


func (sm *ShardMaster) handleMove(op *Op) Config {
	prevConfig := sm.configs[len(sm.configs)-1]
	newConfig := prevConfig.Copy()

	currShard := op.Shard
	swapGroup := op.GID

	newConfig.Shards[currShard] = swapGroup
	return newConfig

}

func (sm *ShardMaster) handleQuery(op *Op) {
	if op.Num < 0 || op.Num >= len(sm.configs) {
		op.config = sm.configs[len(sm.configs)-1].Copy()
	} else {
		op.config = sm.configs[op.Num].Copy()
	}
}

func (sm *ShardMaster) isDuplicateOp(op *Op, idx int) bool {
	//TODO: return true if this request is too late
	lastIdx, ok2 := sm.lastApplied[int(op.Me)]
	if !ok2 || lastIdx < op.ReqId {
		sm.lastApplied[int(op.Me)] = op.ReqId
		sm.lastIncludedIndex = idx
		return false

	}
	return true
}

func (sm *ShardMaster) ApplyRoutine() {
	for {
		msg := <-sm.applyCh
		if msg.UseSnapshot {
			continue
		}

		op := msg.Command.(Op)
		idx := msg.Index

		sm.mu.Lock()
		if op.Type != "Query" && sm.isDuplicateOp(&op, idx) {
			ch, notify := sm.results[idx]
			sm.mu.Unlock()
			if notify {
				select {
				case ch <- op:
				default:
				}
			}
			continue
		}
		if op.Type == "Join" {
			newConfig := sm.handleJoin(&op)
			newConfig.Num = sm.configs[len(sm.configs)-1].Num + 1
			sm.Rebalance(&newConfig)
			sm.configs = append(sm.configs, newConfig)
		} else if op.Type == "Leave" {
			newConfig := sm.handleLeave(&op)
			newConfig.Num = sm.configs[len(sm.configs)-1].Num + 1
			sm.Rebalance(&newConfig)
			sm.configs = append(sm.configs, newConfig)
		} else if op.Type == "Move" {
			newConfig := sm.handleMove(&op)
			newConfig.Num = sm.configs[len(sm.configs)-1].Num + 1
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
		Me: int64(args.Me),
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
		Me: int64(args.Me),
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
		Me: int64(args.Me),
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
	reqId := args.Id
	op := Op{
		Type:  "Query",
		ReqId: int(reqId),
		Num:   args.Num,
		Me: int64(args.Me),
	}

	success, applied := sm.waitAppliedOrTimeout(op)

	if !success {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	reply.Config = applied.config
	reply.WrongLeader = false
	reply.Err = OK
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

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})

	sm.results = make(map[int]chan Op)
	sm.lastApplied = make(map[int]int)
	sm.applyCh = make(chan raft.ApplyMsg, 2048)
	go sm.ApplyRoutine()
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	return sm
}
