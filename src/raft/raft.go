package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	FOLLOWER  int32 = 0
	CANDIDATE int32 = 1
	LEADER    int32 = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A voting related
	// switchCh         chan bool // Signal that a state change just occurred
	timeout          time.Duration
	state            int32
	appendEntryChan  chan bool // only send signal through this channel if the request was valid
	grantVoteChan    chan bool // only send signal through this channel if the request was valid
	receivedVoteChan chan bool
	currentTerm      int
	isLeader         bool
	votedFor         int // Voted for anyone during this term, reset to -1 with each term update
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	term := rf.currentTerm
	leader := rf.isLeader
	rf.mu.Unlock()

	// Your code here (2A).
	return term, leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) setTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.mu.Unlock()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = args.Term
	reply.VoteGranted = false

	// For 2A, vote requestor's log doesn't have to be up-to-date
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	//update term if it's newer and reset voted for
	if rf.currentTerm < args.Term {
		rf.setTerm(args.Term)
	}
	//check if we've voted for anyone in this term. If we have not then we can vote for this one
	if rf.votedFor == -1 {
		reply.VoteGranted = true
		// rf.mu.Lock()
		// defer rf.mu.Unlock()
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm < args.Term {
		rf.setTerm(args.Term)
		reply.Success = true
		rf.appendEntryChan <- true
	} else if rf.currentTerm == args.Term {
		reply.Success = true
		rf.appendEntryChan <- true
	} else {
		reply.Success = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.VoteGranted == true {
		rf.receivedVoteChan <- true
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// rf.mu.Lock()
	if rf.currentTerm < reply.Term {
		rf.setTerm(reply.Term)
	}
	// rf.mu.Unlock()
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER // Every node starts as a follower
	rf.timeout = time.Duration(rand.Intn(151)+150) * time.Millisecond
	// fmt.Printf("Time out :%+v\n", rf.timeout)
	rf.currentTerm = 0
	rf.isLeader = false
	rf.appendEntryChan = make(chan bool, 1)
	rf.grantVoteChan = make(chan bool, 1)
	//Run the raft
	go rf.Execute()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) Execute() {
	for {
		// fmt.Printf("Server %d: State is: %d\n", rf.me, rf.state)
		switch rf.state {
		case FOLLOWER:
			rf.runFollower()
		case CANDIDATE:
			rf.runCandidate()
		case LEADER:
			rf.runLeader()
		}
	}
}

func (rf *Raft) changeState(state int32) {
	atomic.StoreInt32(&rf.state, state)
}

//
// This function should watch for incoming AppendEntries request
// If no such request was made, call the Elect Leader function and wait for its result.
// If such a request was made, just refresh the timer
//

// No premature cancellation will happen to this function, hence a context is not necessary
func (rf *Raft) runFollower() {
	// fmt.Printf("Server %d  Term: %d : Running as follower\n", rf.me, rf.currentTerm)
	timeElapsed := 0 //miliseconds
	for {
		// fmt.Printf("Time elapsed: %d %d\n", timeElapsed, rf.timeout)
		select {
		case <-rf.appendEntryChan:
			// fmt.Printf("Sever %d Term: %d received a heartbeat message\n", rf.me, rf.currentTerm)
			timeElapsed = 0
		case <-rf.grantVoteChan:
			timeElapsed = 0
		default:
			time.Sleep(1 * time.Millisecond)
			timeElapsed++
			if time.Duration(timeElapsed)*time.Millisecond >= rf.timeout { //doesn't need lock
				rf.changeState(CANDIDATE)
				return
			}
		}
	}
}

func (rf *Raft) runCandidate() {
	var voteCount int = 1
	var timer *time.Timer = time.NewTimer(rf.timeout)

	//Lock raft to allocate a new channel
	// rf.mu.Lock()
	rf.receivedVoteChan = make(chan bool, len(rf.peers)-1)
	// rf.mu.Unlock()
	rf.setTerm(rf.currentTerm + 1)

	// fmt.Printf("Server %d  Term: %d : I'm becoming candidate\n", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			go rf.sendRequestVote(i, &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}, &RequestVoteReply{})
		}
	}

	for {
		select {
		case <-timer.C:
			fmt.Println(rf.timeout)
			rf.changeState(CANDIDATE)
			return
		case <-rf.receivedVoteChan:
			voteCount++
		case <-rf.appendEntryChan:
			rf.changeState(FOLLOWER)
			return
		default:
			if voteCount > len(rf.peers)/2 {
				// fmt.Printf("Got %d votes, elected\n", voteCount)
				rf.changeState(LEADER)
				return
			}
		}
	}
}

func (rf *Raft) runLeader() {
	// fmt.Printf("Server %d: I'm becoming Leader			Term: %d \n", rf.me, rf.currentTerm)
	// rf.mu.Lock()
	rf.isLeader = true
	// rf.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// fmt.Printf("Server %d  Term: %d : sending outward hearbeats to other nodes\n", rf.me, rf.currentTerm)
				for i := 0; i < len(rf.peers); i++ {
					if rf.me != i {
						go rf.sendAppendEntries(i, &AppendEntriesArgs{rf.currentTerm}, &AppendEntriesReply{})
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(ctx)

	for {
		select {
		case <-rf.appendEntryChan:
			rf.changeState(FOLLOWER)
			cancel()
			// rf.mu.Lock()
			rf.isLeader = false
			// rf.mu.Unlock()
			return
		}
	}
}
