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
	timeout       time.Duration
	state         int32
	lastHeartBeat time.Time
	switchState   chan int32
	timeoutChan   chan struct{} // only send signal through this channel if the request was valid
	heartBeatChan chan struct{} // only send signal through this channel if the request was valid
	// receivedVoteChan chan bool
	voteCount   int
	currentTerm int
	isLeader    bool
	votedFor    int // Voted for anyone during this term, reset to -1 with each term update
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
	LeaderID int
	Term     int
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

// func (rf *Raft) setTerm(term int) {
// 	// rf.mu.Lock()
// 	rf.currentTerm = term
// 	rf.votedFor = -1
// 	rf.voteCount = 1
// 	// rf.mu.Unlock()
// }

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = args.Term

	// For 2A, vote requestor's log doesn't have to be up-to-date
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Server %d  Term: %d : Received a vote request from %d for term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//update term if it's newer and reset voted for
	if rf.currentTerm < args.Term {
		// rf.setTerm(args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		//todo: switch state
		rf.switchState <- FOLLOWER
	}
	//check if we've voted for anyone in this term. If we have not then we can vote for this one
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		fmt.Printf("Server %d  Term: %d : granted vote to %d for term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.lastHeartBeat = time.Now() //if vote was granted, also reset the timer
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("Server %d  Term: %d : Received a heartbeat msg from %d for term %d\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Success = true
	reply.Term = args.Term
	if rf.currentTerm < args.Term {
		// rf.setTerm(args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		//todo: switch state
		rf.switchState <- FOLLOWER
	}
	rf.lastHeartBeat = time.Now()

	// // Send to heartBeatChan if it doesn't block
	// // If it does, no sending is needed
	// select {
	// case rf.heartBeatChan <- struct{}{}:
	// 	return
	// default:
	// 	return
	// }
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if reply.VoteGranted == true /*|| reply.Term == rf.currentTerm */ {
		rf.voteCount++
	} else if reply.Term > rf.currentTerm {
		// rf.setTerm(reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		//todo: switch state
		rf.switchState <- FOLLOWER
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if rf.currentTerm < reply.Term {
		// rf.setTerm(reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		//todo: switch state
		rf.switchState <- FOLLOWER
	}
	rf.mu.Unlock()
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).

	return -1, -1, false
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
	// rf.timeout = time.Duration(rand.Intn(151)+150) * time.Millisecond
	rf.timeout = time.Duration(rand.Intn(2000)+1000) * time.Millisecond
	rf.currentTerm = 0
	rf.isLeader = false
	rf.lastHeartBeat = time.Now()
	rf.switchState = make(chan int32)
	//Run the raft
	go rf.Execute()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) Execute() {
	for {
		// fmt.Printf("Server %d: State is: %d\n", rf.me, rf.state)

		f := func(cancel context.CancelFunc, ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case state := <-rf.switchState:
				cancel()
				rf.changeState(state)
			}
		}

		ctx, cancel := context.WithCancel(context.Background())

		switch s := atomic.LoadInt32(&rf.state); s {
		case FOLLOWER:
			go f(cancel, ctx)
			rf.runFollower(ctx, cancel)
		case CANDIDATE:
			go f(cancel, ctx)
			rf.runCandidate(ctx, cancel)
		case LEADER:
			go f(cancel, ctx)
			rf.runLeader(ctx, cancel)
		}
	}
}

func (rf *Raft) changeState(state int32) {
	atomic.StoreInt32(&rf.state, state)
}

// func (rf *Raft) timeoutWatch(ctx context.Context) {
// 	for {
// 		rf.mu.Lock()
// 		timeElapsed := time.Now().Sub(rf.lastHeartBeat)
// 		rf.mu.Unlock()

// 		if timeElapsed >= rf.timeout {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case rf.timeoutChan <- struct{}{}:
// 				return
// 			}
// 		}
// 		time.Sleep(1 * time.Millisecond)
// 	}
// }

//
// This function should watch for incoming AppendEntries request
// If no such request was made, call the Elect Leader function and wait for its result.
// If such a request was made, just refresh the timer
//

// No premature cancellation will happen to this function, hence a context is not necessary
func (rf *Raft) runFollower(ctx context.Context, cancel context.CancelFunc) {
	fmt.Printf("Server %d  Term: %d : I'm becoming follower\n", rf.me, rf.currentTerm)
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	// go rf.timeoutWatch(ctx)
	// select {
	// case <-rf.timeoutChan:
	// 	rf.changeState(CANDIDATE)
	// }

	for {
		rf.mu.Lock()
		timeElapsed := time.Now().Sub(rf.lastHeartBeat)
		rf.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		default:
			if timeElapsed >= rf.timeout {
				cancel()
				rf.changeState(CANDIDATE)
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (rf *Raft) watchVotes(done chan struct{}) chan struct{} {
	c := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				rf.mu.Lock()
				voteCount := rf.voteCount
				rf.mu.Unlock()
				if voteCount > len(rf.peers)/2 {
					c <- struct{}{}
					return
				}
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	return c
}

/*
	Increment term and send request vote
	periodically after each 1ms, check the votecount.
	If the number of votes exceed the majority change to leader state
	else keep waiting until either round timeout or gets new vote
*/
func (rf *Raft) runCandidate(ctx context.Context, cancel context.CancelFunc) {
	//Lock raft to allocate a new channel
	rf.mu.Lock()
	// rf.setTerm(rf.currentTerm + 1)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()

	fmt.Printf("Server %d  Term: %d : I'm becoming candidate\n", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			rf.mu.Lock()
			term := rf.currentTerm
			me := rf.me
			rf.mu.Unlock()
			go rf.sendRequestVote(i, &RequestVoteArgs{Term: term, CandidateId: me}, &RequestVoteReply{})
		}
	}
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	// go rf.timeoutWatch(ctx)

	// for {
	// 	rf.mu.Lock()
	// 	// fmt.Printf("Server: %d Term: %d Time is: %d\n", rf.me, rf.currentTerm, rf.lastHeartBeat.UnixNano())
	// 	votes := rf.voteCount
	// 	rf.mu.Unlock()
	// 	if votes <= len(rf.peers)/2 {
	// 		select {
	// 		case <-rf.timeoutChan:
	// 			rf.changeState(CANDIDATE)
	// 			return
	// 		case <-rf.heartBeatChan:
	// 			// fmt.Println("Received heartbeat from new leader, reverting back to follower state")
	// 			rf.changeState(FOLLOWER)
	// 			return
	// 		default:
	// 			//Nothing happens
	// 		}
	// 	} else {
	// 		rf.changeState(LEADER)
	// 		return
	// 	}

	// 	time.Sleep(1 * time.Millisecond)
	// }

	timer := time.NewTimer(rf.timeout)

	done := make(chan struct{})
	defer close(done)
	elected := rf.watchVotes(done)

	select {
	case <-elected:
		cancel()
		rf.changeState(LEADER)
		return
	case <-timer.C:
		cancel()
		rf.changeState(CANDIDATE)
		return
	case <-ctx.Done():
		return
	}
}

func (rf *Raft) runLeader(ctx context.Context, cancel context.CancelFunc) {
	fmt.Printf("Server %d Term: %d: I'm becoming Leader\n", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.isLeader = true
	rf.mu.Unlock()

	done := make(chan struct{})
	defer close(done)

	go func(done chan struct{}) {
		for {
			select {
			case <-done:
				return
			default:
				fmt.Printf("Server %d  Term: %d : sending outward hearbeats to other nodes\n", rf.me, rf.currentTerm)
				for i := 0; i < len(rf.peers); i++ {
					if rf.me != i {
						rf.mu.Lock()
						term := rf.currentTerm
						me := rf.me
						rf.mu.Unlock()
						go rf.sendAppendEntries(i, &AppendEntriesArgs{Term: term, LeaderID: me}, &AppendEntriesReply{})
					}
				}
				time.Sleep(150 * time.Millisecond)
			}
		}
	}(done)

	select {
	case <-ctx.Done():
		rf.mu.Lock()
		rf.isLeader = false
		rf.mu.Unlock()
		return
	}
}
