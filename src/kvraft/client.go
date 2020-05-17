package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	id 		int64
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
	ck.id = nrand()
	return ck
}

func (ck *Clerk) Get(key string) string {
	/*--------------------------------------------------------------------*
	 * Retreives the value mapped to by key. Tries each server in turn    *
	 * until it finds a leader. In case of ErrWrongTerm, si is still the  *
	 * leader but not in the same term, so we do not change si for the    *
	 * next loop iteration. Otherwise, we always increment si. 			  *
	 *--------------------------------------------------------------------*/
	 args := GetArgs{
	 	ck.id,
		key,
	 }

	 for {
		si := 0
		for (si < len(ck.servers)) {
			reply := GetReply{}
			ok := ck.servers[si].Call("KVServer.Get", &args, &reply)
			if (ok && reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			} else if ((!ok) || reply.Err != ErrWrongTerm) {
				si++
			}
		}
	 }

	 return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	/*--------------------------------------------------------------------*
	 * Sends a Put/Append request for consensus, and returns when consen- *
	 * sus has been acheived.  											  *
	 *--------------------------------------------------------------------*/
	 args := PutAppendArgs{
	 	ck.id,
	 	nrand(),
		key,
		value,
		op,
	 }

	 for {
		si := 0
		for (si < len(ck.servers)) {
			reply := PutAppendReply{}
			ok := ck.servers[si].Call("KVServer.PutAppend", &args, &reply)
			if (ok && reply.Err == OK) {
				return 
			} else if ((!ok) || reply.Err != ErrWrongTerm) {
				si++
			}
		}
	 }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
