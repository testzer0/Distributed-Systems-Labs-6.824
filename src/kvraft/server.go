package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
)
import "time"
import "context"
import "crypto/sha256"
import "fmt"
import "bytes"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	ListenChan 		chan bool 		 	// channel to listen on for commit()ed messages
	TypeOfMsg 		string 				// type of message
	Key 			string 				// used by PutAppend and Get
	Value 			string 				// used by PutAppend
	Id 				int64
	ClerkId 		int64
}

type KVServer struct {
	mu     			sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg
	dead    		int32 				// set by Kill()
	maxraftstate 	int 				// snapshot if log grows this big

	LastCommit 		map[int64]int64 	// maps clerk to its last commit to prevent double commit
	Committed 		map[string]bool 	// whether an op with said hash has been Committed
	KVStore 		map[string]string 	// Committed KV pairs
	Reserved 		int64 				// increasing seq.no. for Committed entries
}

func asSha256(o interface{}) string {
    h := sha256.New()
    h.Write([]byte(fmt.Sprintf("%v", o)))

    return fmt.Sprintf("%x", h.Sum(nil))
}

func (kv *KVServer) StartConsensusAndWait(op Op) int {
	/*--------------------------------------------------------------------*
	 * Starts consensus on said op and waits for raft peers to convene.   *
	 * If consensus fails due to any reason, returns a bare-bones error   *
	 * code. kv.mu.Lock() must NOT be held when calling this function.    *
	 *--------------------------------------------------------------------*/

	 listen := op.ListenChan

	 _, term, leader := kv.rf.Start(op)

	 if (!leader) {
	 	return 1
	 }

	 for {
	 	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Millisecond)

	 	select {
	 		case <-ctx.Done():
	 			cancel()
	 			nowTerm, stillLeader := kv.rf.GetState()
	 			if (!stillLeader) {
	 				return 1
	 			} else if (nowTerm != term) {
	 				return 2
	 			} else {
	 				continue
	 			}

	 		case <-listen:
	 			cancel()
	 			return 0
	 	}
	 }

	 return 0

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	op := Op{
		make(chan bool),
		"Get",
		args.Key,
		"",
		0,
		0,
	}

	kv.mu.Unlock()

	ret := kv.StartConsensusAndWait(op)

	if (ret == 0) { 		// success
		reply.Err = OK
		kv.mu.Lock()

		value, ok := kv.KVStore[args.Key]
		if(!ok) {
			value = ""
			reply.Err = ErrNoKey
		}
		reply.Value = value

		kv.mu.Unlock()
	} else if (ret == 1) {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = ErrWrongTerm
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	if kv.LastCommit[args.ClerkId] == args.Id {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	op := Op{
		make(chan bool),
		args.Op,
		args.Key,
		args.Value,
		args.Id,
		args.ClerkId,
	}

	kv.mu.Unlock()

	ret := kv.StartConsensusAndWait(op)

	if (ret == 0) {
		reply.Err = OK
	} else if (ret == 1) {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = ErrWrongTerm
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) TakeSnapshotAndSend(index int) {
	/*----------------------------------------------------------------------------------*
	 * Takes a snapshot of the current state and sends it to kv.rf for persisting, when *
	 * the raft state grows too big. kv.mu.Lock() must be held when calling this func.  *
	 *----------------------------------------------------------------------------------*/

	 buf := new(bytes.Buffer)
	 encoder := labgob.NewEncoder(buf)
	 encoder.Encode(kv.LastCommit)
	 // encoder.Encode(kv.Committed)
	 encoder.Encode(kv.KVStore)
	 encoder.Encode(kv.Reserved)

	 go kv.rf.CreateSnapshot(buf.Bytes(), index)
}

func (kv *KVServer) HandlePut(key string, value string) {
	/*-----------------------------------------------------------------------------------*
	 * Handles a single Put message Committed by kv.rf. kv.mu.Lock() must be held when    *
	 * calling this function. 															 *
	 *-----------------------------------------------------------------------------------*/

	 kv.KVStore[key] = value
}

func (kv *KVServer) HandleAppend(key string, value string) {
	/*-----------------------------------------------------------------------------------*
	 * Handles a single Append message Committed by kv.rf. kv.mu.Lock() must be held when *
	 * calling this function. 															 *
	 *-----------------------------------------------------------------------------------*/

	 prevValue, ok := kv.KVStore[key]
	 if (!ok) {
	 	prevValue = ""
	 }

	 newValue := prevValue + value
	 kv.KVStore[key] = newValue
}

func (kv *KVServer) RestoreState(snapshot []byte) {
	/*-----------------------------------------------------------------------------------*
	 * Restores state from a previously taken snapshot. 								 *
	 *-----------------------------------------------------------------------------------*/
	var fI, fT int 

	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)

	decoder.Decode(&fI)
	decoder.Decode(&fT)
	decoder.Decode(&kv.LastCommit)
	// decoder.Decode(&kv.Committed)
	decoder.Decode(&kv.KVStore)
	decoder.Decode(&kv.Reserved)
}

func (kv *KVServer) HandleOneMsg(op raft.ApplyMsg) {
	/*-----------------------------------------------------------------------------------*
	 * Handles a single ApplyMsg Committed by kv.rf.  								     *
	 *-----------------------------------------------------------------------------------*/

	 kv.mu.Lock()
	 defer kv.mu.Unlock()

	 msg := op.Command

	 if (!op.CommandValid) {
	 	kv.RestoreState(msg.([]byte))
	 	return
	 }

	 ToApply, _ := msg.(Op)

	 digest := asSha256(ToApply)
	 if _, ok := kv.Committed[digest]; ok {
	 	go func(ch chan bool) {
	 		ch <- true
	 	} (ToApply.ListenChan)
	 	return
	 }

	 if (ToApply.TypeOfMsg == "Put" && kv.LastCommit[ToApply.ClerkId] != ToApply.Id) {
	 	kv.HandlePut(ToApply.Key, ToApply.Value)
	 	kv.LastCommit[ToApply.ClerkId] = ToApply.Id
	 } else if (ToApply.TypeOfMsg == "Append" && kv.LastCommit[ToApply.ClerkId] != ToApply.Id) {
	 	kv.HandleAppend(ToApply.Key, ToApply.Value)
	 	kv.LastCommit[ToApply.ClerkId] = ToApply.Id
	 } else {
	 	// do nothing for get
	 }

	 // might as well also check for raft state size
	 if (kv.maxraftstate > 0 && kv.rf.GetRaftStateSize() > kv.maxraftstate) {
	 	kv.TakeSnapshotAndSend(op.CommandIndex)
	 }

	 kv.Committed[digest] = true
	 go func(ch chan bool) {
	 	ch <- true
	 } (ToApply.ListenChan)
}

func (kv *KVServer) ListenLoop() {
	/*-----------------------------------------------------------------------------------*
	 * Runs forever and listens for incoming Committed messages, applying them to state. *
	 *-----------------------------------------------------------------------------------*/

	 forever := make(chan bool)

	 go func(kv *KVServer) {
	 	for msg := range kv.applyCh {
	 		kv.HandleOneMsg(msg)
	 	}
	 } (kv)

	 <-forever

}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.LastCommit = make(map[int64]int64)
	kv.Committed = make(map[string]bool)
	kv.KVStore = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ListenLoop()

	return kv
}
