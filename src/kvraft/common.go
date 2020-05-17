package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongTerm   = "ErrWrongTerm"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClerkId 	int64
	Id 			int64
	Key   		string
	Value 		string
	Op    		string 			// "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClerkId		int64
	Key 		string
}

type GetReply struct {
	Err   Err
	Value string
}
