package raft


import "sync"
import "sync/atomic"
import "../labrpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "time"
import "sort"
import "context"
import "bytes"
import "../labgob"



type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command 	interface{}
	TermRecvd 	int 			  // If this is 0 then this signifies the 0-index entry (NOT TO BE COMMITED)
}

const (							  // useful macros
	FOLLOWER 	= 	"FOLLOWER"
	CANDIDATE  	= 	"CANDIDATE"
	LEADER 		= 	"LEADER"
	OK 			=   "OK"
	MISMATCH 	=   "MISMATCH"
	TERM 		=   "TERM"
)


type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me        		int                 // this peer's index into peers[]
	dead      		int32               // set by Kill()
	apply_chan 		chan ApplyMsg 		// Used to send committed messages

	currentTerm 	int 		  		// current term as believed by rf
	votedFor 		int 		  		// the peer which recv'd our vote this term
	log 			[]interface{} 	  	// log
	recv_term 		[]int 				// log terms

	state 			string
	commitIndex 	int
	lastApplied 	int 				// index of last entry applied to state
	nextIndex 		[]int 
	matchIndex 		[]int
	timeout_reference 	int64
	lastEmpty 		int64
	randomPenalty 	int 				// random backoff if we lose election between 400-800 ms
	randomTimeout   int 				// random election timer
}

func doNothing() {
	fmt.Printf("Do Nothing\n") 	  		// Just here so that golang doesnt scream about fmt being unused
}

func randInt32(lower int, upper int) int {
	/*--------------------------------------------------------------------*
	 * returns a uniformly pseudorandom value in [lower, upper). 		  *
	 *--------------------------------------------------------------------*/

	 if (lower >= upper) {
	 	return upper
	 }
	 var diff int64 = int64(upper - lower)
	 random, _ := rand.Int(rand.Reader, big.NewInt(diff))
	 ret := int(random.Int64())
	 return ret + lower
}


func (rf *Raft) GetState() (int, bool) {
	/*---------------------------------------------------------------------*
	 * Returns the current term and whether the peer is the leader, as     *
	 * believed by the raft peer. 										   *
	 *---------------------------------------------------------------------*/


	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := (rf.state == LEADER)
	
	return term, isleader
}


func (rf *Raft) persist() {
	/*-----------------------------------------------------------------------*
	 * Stores nonvolatile state to disk, so that recovery can be done in     *
	 * case of failure. rf.mu.Lock() must be held when calling this routine. *
	 *-----------------------------------------------------------------------*/
	 
	 buf := new(bytes.Buffer)
	 encoder := labgob.NewEncoder(buf)

	 encoder.Encode(rf.currentTerm)
	 encoder.Encode(rf.votedFor)
	 encoder.Encode(rf.log)
	 encoder.Encode(rf.recv_term)

	 data := buf.Bytes()
	 rf.persister.SaveRaftState(data)
}


func (rf *Raft) readPersist(data []byte) {
	/*------------------------------------------------------------------*
	 * Restores previously persisted state. 							*
	 *------------------------------------------------------------------*/
	 if data == nil || len(data) < 1 { // bootstrap without any state?
	 	return
	 }
	 
	 var cT int 
	 var vF int 
	 var l []interface{}
	 var r_t []int 

	 buf := bytes.NewBuffer(data)
	 d := labgob.NewDecoder(buf)

	 if (d.Decode(&cT) != nil ||
	 	 d.Decode(&vF) != nil ||
	 	 d.Decode(&l) != nil  ||
	 	 d.Decode(&r_t) != nil) {
	 	 	fmt.Printf("Fatal! readPersist failed\n")
	 	 } else {
	 	 	rf.currentTerm = cT 
	 	 	rf.votedFor = vF
	 	 	rf.log = append(l[:0:0], l...)
	 	 	rf.recv_term = append(r_t[:0:0], r_t...)
	 	 }
}



type RequestVoteArgs struct {
	Term 			int 
	CandidateId		int 
	LastLogIndex 	int 
	LastLogTerm 	int
}

type RequestVoteReply struct {
	Term 			int 
	VoteGranted 	bool
}

type AppendEntriesArgs struct {
	Term 			int 
	LeaderId 		int 
	PrevLogIndex 	int 
	PrevLogTerm 	int
	Entries 		[]interface{}
	Terms 			[]int
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term 			int 
	Err 	 		string
	OptTerms 		[]int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	if (args.Term > rf.currentTerm) {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	my_last_index := len(rf.log)-1
	my_last_term := rf.recv_term[my_last_index]

	reply.VoteGranted = false 
	if (rf.currentTerm <= args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		if (args.LastLogTerm > my_last_term ||
		   (args.LastLogTerm == my_last_term && args.LastLogIndex >= my_last_index)) {
		   	reply.VoteGranted = true
		   	rf.votedFor = args.CandidateId
		   	rf.timeout_reference = time.Now().UnixNano() / 1000000
		   	rf.randomTimeout = randInt32(500, 600)
		   } 
	}
}

func (rf *Raft) sendRequestVoteHelper(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan bool) {
	/*-----------------------------------------------------------------------------------------*
	 * Does the actual work of sending the RequestVote. Should be called only by sendRequest-  *
	 * Vote.																				   *
	 *-----------------------------------------------------------------------------------------*/
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	ch <- ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	/*-----------------------------------------------------------------------------------------*
	 * Sends a RequestVote to said peer with a timeout of 50 ms. 							   *
	 *-----------------------------------------------------------------------------------------*/
     
     ch := make(chan bool)
	 ctx, cancel := context.WithTimeout(context.Background(), time.Duration(50)*time.Millisecond)

	 go rf.sendRequestVoteHelper(server, args, reply, ch)

	 select{
	 	case <-ctx.Done():
	 		return false 
	 	case get := <-ch:
	 		cancel()
	 		return get
	 }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*-----------------------------------------------------------------------------------------*
	 * Handles a single AppendEntries request from the leader. 								   *
	 *-----------------------------------------------------------------------------------------*/

	 rf.mu.Lock()
	 defer rf.mu.Unlock()
	 defer rf.persist()

	 reply.Term = rf.currentTerm
	 if (args.Term > rf.currentTerm) {
	 	rf.currentTerm = args.Term
	 	rf.votedFor = -1
	 	rf.state = FOLLOWER
	 }

	 if (args.Term < rf.currentTerm) {
	 	reply.Err = TERM
	 	return
	 }

	 rf.timeout_reference = time.Now().UnixNano() / 1000000
	 rf.randomTimeout = randInt32(500, 600)

	 if (args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.recv_term[args.PrevLogIndex]) {
	 	reply.Err = MISMATCH
	 	reply.OptTerms = append(rf.recv_term[:0:0], rf.recv_term...)
	 	return
	 }

	 confict_index := args.PrevLogIndex+1
	 for confict_index < len(rf.log) && confict_index <= args.PrevLogIndex + len(args.Entries) {
	 	if (rf.log[confict_index] != args.Entries[confict_index - args.PrevLogIndex - 1]) {
	 		break
	 	}
	 	confict_index++
	 }

	 if (confict_index < len(rf.log)) {
	 	rf.log = rf.log[:confict_index]
	 	rf.recv_term = rf.recv_term[:confict_index]
	 }

	 for confict_index <= args.PrevLogIndex + len(args.Entries) {
	 	rf.log = append(rf.log, args.Entries[confict_index - args.PrevLogIndex - 1])
	 	rf.recv_term = append(rf.recv_term, args.Terms[confict_index - args.PrevLogIndex - 1])
	 	confict_index++
	 }

	 if (args.LeaderCommit > rf.commitIndex) {
	 	commit := args.PrevLogIndex + len(args.Entries)
	 	if (commit > args.LeaderCommit) {
	 		commit = args.LeaderCommit
	 	}
	 	rf.commitIndex = commit
	 } 

	 reply.Err = OK
}

func (rf *Raft) sendAppendEntriesHelper(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) {
	/*-----------------------------------------------------------------------------------------*
	 * Does the heavy work of sending the AppendEntries. Should be called only by sendAppend-  *
	 * Entries and only as a goroutine. 													   *
	 *-----------------------------------------------------------------------------------------*/
	 ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	 ch <- ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	/*-----------------------------------------------------------------------------------------*
	 * Sends an AppendEntries msg to the said server, and returns whether the server replied.  *
	 * rf.mu.Lock() need not be held to call this function: it is deliberate that it does no   *
	 * housekeeping tasks such as update term. Times out at 100 ms 							   *											       *
	 *-----------------------------------------------------------------------------------------*/
	
	 ch := make(chan bool)
	 ctx, cancel := context.WithTimeout(context.Background(), time.Duration(100)*time.Millisecond)

	 go rf.sendAppendEntriesHelper(server, args, reply, ch)

	 select{
	 	case <-ctx.Done():
	 		return false 
	 	case get := <-ch:
	 		cancel()
	 		return get
	 }
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	/*---------------------------------------------------------------------------*
	 * Starts consensus on command if rf is the leader. Returns term, and if     *
	 * rf is leader, to the caller.	 											 *
	 *---------------------------------------------------------------------------*/

	 rf.mu.Lock()
	 defer rf.mu.Unlock()

	 term := rf.currentTerm
	 isLeader := (rf.state == LEADER)
	 index := -1
	 if (isLeader) {
	 	index = len(rf.log)
	 	rf.log = append(rf.log, command)
	 	rf.recv_term = append(rf.recv_term, term)
	 	rf.matchIndex[rf.me] = len(rf.log) - 1
	 	rf.persist()
	 } 

	 return index, term, isLeader
}

func (rf *Raft) start_election_cycle() {
	/*--------------------------------------------------------------------------*
	 * Starts an election. If succeeds then performs leader routines. If fails, *
	 * sets a random backoff penalty and returns. rf.mu.Lock() must be held     *
	 * calling this routine, since we must not allow other functions to change  *
	 * to follower on finding a higher term in between the function call. 		*
	 *--------------------------------------------------------------------------*/

	 rf.state = CANDIDATE
	 rf.currentTerm++
	 numVotes := 1 		// vote for self
	 rf.votedFor = rf.me
	 rf.persist()

	 rf.timeout_reference = time.Now().UnixNano() / 1000000
	 rf.randomTimeout = randInt32(500, 600)


	 args := RequestVoteArgs{
	 	rf.currentTerm,
	 	rf.me,
	 	len(rf.log)-1,
	 	rf.recv_term[len(rf.log)-1],
	 }

	 rf.mu.Unlock() 		// dont need lock for a bit

	 for i := 0; i < len(rf.peers); i++ {
	 	if (i == rf.me) {
	 		continue
	 	}
	 	reply := RequestVoteReply{}
	 	ok := rf.sendRequestVote(i, &args, &reply)

	 	if (rf.state == FOLLOWER) {
	 		break
	 	}
	 	rf.mu.Lock()

	 	if (ok && reply.VoteGranted) {
	 		numVotes++
	 	} else if (ok && reply.Term > rf.currentTerm) {
	 		rf.currentTerm = reply.Term 
	 		rf.state = FOLLOWER
	 		rf.votedFor = -1
	 		rf.persist()
	 		rf.mu.Unlock()
	 		break
	 	}
	 	rf.mu.Unlock()
	 }

	 rf.mu.Lock() 				// watch_for_timeout expects lock to be held when returning
	 if (rf.state == FOLLOWER) {
	 	return 					
	 } else if (2*numVotes > len(rf.peers)) {
	 	rf.state = LEADER
	 	for i := 0; i < len(rf.peers); i++ {
	 		rf.matchIndex[i] = 0 			// We don't know what prev leaders did; assume nothing matches for now
	 		rf.nextIndex[i] = len(rf.log)
	 	}
	 	rf.matchIndex[rf.me] = len(rf.log) - 1
	 	rf.send_empty_heartbeats()
	 	return
	 } 
}


func (rf *Raft) watch_for_timeout() {
	/*---------------------------------------------------------------------------*
	 * Polls the raft server every 100 ms or so (slightly more) to check if      *
	 * timer has expired. If so, converts to candidate. 						 *
	 *---------------------------------------------------------------------------*/

	 for {
	 	if (rf.dead == 1) {
	 		return 
	 	}
	 	rf.mu.Lock()
	 	if (rf.state == FOLLOWER || rf.state == CANDIDATE) { 
	 		current_time := time.Now().UnixNano() / 1000000
	 		if (current_time - rf.timeout_reference > int64(rf.randomTimeout)) {
	 			rf.start_election_cycle()
	 		}
	 		
	 	} else {
	 		current_time := time.Now().UnixNano() / 1000000
	 		if (current_time - rf.timeout_reference > int64(rf.randomTimeout)) {
	 			rf.timeout_reference = current_time
	 			rf.randomTimeout = randInt32(500, 600)
	 		}
	 	}

	 	rf.mu.Unlock()
	 	time.Sleep(10 * time.Millisecond)
	 	
	 }
}

func (rf *Raft) send_empty_heartbeats() {
	/*--------------------------------------------------------------------*
	 * Sends empty heartbeats to all peers to maintain authority.         *
	 * rf.mu.Lock() must be held when calling this routine. 		  	  *
	 *--------------------------------------------------------------------*/


	 if (rf.state != LEADER) {
	 	return
	 }

	 for i := 0; i < len(rf.peers); i++ {
	 	if (i == rf.me) {
	 		continue
	 	}
	 	args := AppendEntriesArgs {
	 		rf.currentTerm,
	 		rf.me,
	 		rf.nextIndex[i] - 1,
	 		rf.recv_term[rf.nextIndex[i]-1],
	 		make([]interface{}, 0),
	 		make([]int, 0),
	 		rf.commitIndex,
	 	}
	 	reply := AppendEntriesReply{}
	 	rf.mu.Unlock()
	 	ok := rf.sendAppendEntries(i, &args, &reply)
	 	rf.mu.Lock()
	 	if (rf.state != LEADER) {
	 		break
	 	}
	 	if (ok) {
	 		if (reply.Err == OK) {
	 			// Do nothing
	 		} else if (reply.Err == MISMATCH) {
	 			for (rf.nextIndex[i] > len(reply.OptTerms) || rf.recv_term[rf.nextIndex[i]-1] != reply.OptTerms[rf.nextIndex[i]-1]) {
	 				rf.nextIndex[i]--
	 			}
	 		} else if (reply.Err == TERM && reply.Term > rf.currentTerm) {
	 			// step down
	 			rf.currentTerm = reply.Term
	 			rf.state = FOLLOWER
	 			rf.votedFor = -1
	 			rf.persist()
	 			break
	 		}
	 	}
	 }

	 rf.lastEmpty = time.Now().UnixNano() / 1000000

}

func (rf *Raft) server_routines() {
	/*--------------------------------------------------------------------*
	 * Routines for all servers; runs in an infinite loop and checks for  *
	 * stuff that's to be done. 										  *
	 *--------------------------------------------------------------------*/

	 for {
	 	if (rf.dead == 1) {
	 		return
	 	}

	 	rf.mu.Lock()

	 	for (rf.commitIndex > rf.lastApplied) {
	 		rf.lastApplied++
	 		apply_msg := ApplyMsg{
	 			true,
	 			rf.log[rf.lastApplied],
	 			rf.lastApplied,
	 		}

	 		rf.apply_chan <- apply_msg
	 	}

	 	rf.mu.Unlock()
	 	time.Sleep(10 * time.Millisecond)
	 }
}

func (rf *Raft) leader_routines() {
	/*--------------------------------------------------------------------*
	 * Routines for leaders. 											  *
	 *--------------------------------------------------------------------*/
	 for {

	 	if (rf.dead == 1) {
			return
		}

		if (rf.state != LEADER) {
			time.Sleep(time.Duration(30) * time.Millisecond)
			continue
		}

		rf.mu.Lock()

		current_time := time.Now().UnixNano() / 1000000
		if (current_time - rf.lastEmpty > 100) {
			rf.send_empty_heartbeats()
		}

		if(rf.state == LEADER) {
			// if empty heartbeats didnt cost us our leadership
			for i := 0; i < len(rf.peers); i++ {
				if (i == rf.me) {
					continue
				}
				if (rf.nextIndex[i] < len(rf.log)) {
					args := AppendEntriesArgs{
						rf.currentTerm,
	 					rf.me,
	 					rf.nextIndex[i] - 1,
	 					rf.recv_term[rf.nextIndex[i]-1],
	 					rf.log[rf.nextIndex[i]:],
	 					rf.recv_term[rf.nextIndex[i]:],
	 					rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					rf.mu.Unlock() 								 // need to release to avoid 2 way deadlock
					ok := rf.sendAppendEntries(i, &args, &reply)
					rf.mu.Lock()
					if (rf.state != LEADER) {
						break
					}
	 				if (ok) {
	 					if (reply.Err == OK) {
	 						rf.nextIndex[i] = len(rf.log)
	 						rf.matchIndex[i] = len(rf.log) - 1
	 					} else if (reply.Err == MISMATCH) {
	 						for (rf.nextIndex[i] > len(reply.OptTerms) || rf.recv_term[rf.nextIndex[i]-1] != reply.OptTerms[rf.nextIndex[i]-1]) {
	 							rf.nextIndex[i]--
	 						}
	 					} else if (reply.Err == TERM && reply.Term > rf.currentTerm) {
	 						// step down
	 						rf.currentTerm = reply.Term
	 						rf.state = FOLLOWER
	 						rf.votedFor = -1
	 						rf.persist()
	 						break
	 					}
	 				}
				}
			}

		}

		if (rf.state == LEADER) { 		// If none of the AppendEntries cost us our leadership
			sorted := append(rf.matchIndex[:0:0], rf.matchIndex...)
			sort.Ints(sorted)

			// pick the (first if even) middle element's matchIndex and decrement till entry is of this term
			logIndex := sorted[(len(rf.peers)-1)/2]
			for rf.recv_term[logIndex] != rf.currentTerm && logIndex > rf.commitIndex {
				logIndex--
			}
			rf.commitIndex = logIndex
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	 }
}



func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.apply_chan = applyCh
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]interface{}, 0)
	rf.recv_term = make([]int, 0)

	var le interface{}									// do not commit
	rf.log = append(rf.log, le)
	rf.recv_term = append(rf.recv_term, 0)

	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeout_reference = time.Now().UnixNano()/1000000  		// Time in milliseconds rounded down
	rf.randomTimeout = randInt32(500, 600)

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.watch_for_timeout()
	go rf.leader_routines()
	go rf.server_routines()

	return rf
}
