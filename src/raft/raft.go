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
	"bytes"
	"src/labgob"

	//	"bytes"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"src/labgob"
	"src/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type stateType int

const (
	Leader stateType = iota
	Candidate
	Follower
)
const (
	heartBeatTime = 100 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTime  time.Time  // 2A
	heartbeatTime time.Time  // 2A
	currentTerm   int        // 2A
	state         stateType  // 2A
	votedFor      int        // 2A
	log           []LogEntry // 2A

	commitIndex int   // 2B
	lastApplied int   // 2B
	nextIndex   []int // 2B
	matchIndex  []int // 2B
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.Save(data, nil)
	DPrintf("server %d persist data in term %d\n", rf.me, rf.currentTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.mu.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 2A
	CandidateId  int // 2A
	LastLogIndex int // 2A
	LastLogTerm  int // 2A
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 2A
	VoteGranted bool // 2A
}

type AppendEntriesArgs struct {
	Term         int // 2A
	LeadId       int // 2A
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int  // 2A
	Success bool // 2A
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d receive RequestVote from server %d in term %d\n", rf.me, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// 如果请求的任期小于当前任期，则拒绝投票
		return
	}
	if args.Term > rf.currentTerm {
		// 如果请求的任期大于当前任期，则更新任期
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 如果当前节点没有投票或者已经投票给了请求的节点，则投票给请求的节点
		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		// 如果请求的日志任期大于当前节点的日志任期，则投票
		// 如果请求的日志任期等于当前节点的日志任期，且请求的日志索引大于等于当前节点的日志索引，则投票
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			// 重置选举超时时间
			rf.resetElectionTime()
			DPrintf("server %d vote for server %d in term %d\n", rf.me, args.CandidateId, args.Term)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d receive AppendEntries from server %d in term %d\n", rf.me, args.LeadId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	// 如果请求的任期小于当前任期，则拒绝
	if args.Term < rf.currentTerm {
		return
	}
	// 如果请求的任期大于当前任期，则更新任期，并转为Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	// 重置选举超时时间
	rf.resetElectionTime()
	// 如果日志不匹配，则拒绝
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}
	// [PrevLogIndex+1,end)都去掉，然后追加Entries
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	reply.Success = true
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有Leader才能接收客户端的请求
	if rf.state != Leader {
		return index, term, false
	}
	index = len(rf.log)
	// 将命令追加到本地日志中
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist()
	term = rf.currentTerm
	DPrintf("server %d receive client request, append log in term %d, index %d\n", rf.me, term, index)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) appendEntriesTicker() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond)
		func() {
			rf.mu.Lock()
			// 1. 如果不是Leader，则不需要发送心跳
			// 2. 如果上一次发送心跳的时间还没有超过心跳间隔，则不需要发送心跳
			if rf.state != Leader || time.Now().Before(rf.heartbeatTime) {
				rf.mu.Unlock()
				return
			}
			// 重置心跳时间，当前时间加上100ms
			rf.heartbeatTime = time.Now().Add(heartBeatTime)
			// 释放锁，rpc调用过程中不应该持有锁
			rf.mu.Unlock()
			// 给其他节点发送心跳
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					// 更新一下自己的nextIndex和matchIndex
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = len(rf.log) - 1
					continue
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeadId:       rf.me,
					LeaderCommit: rf.commitIndex,
					Entries:      rf.log[rf.nextIndex[i]:],
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				}
				go func(server int, args *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(server, args, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						// 当前节点已经不是Leader，则不需要发送心跳，提前返回
						if rf.state != Leader {
							return
						}
						// 如果心跳的任期小于当前任期，则不需要处理,说明是过期的心跳
						if reply.Term < rf.currentTerm {
							return
						}
						// 发现更高的任期，则转变为Follower
						if rf.currentTerm < reply.Term {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							DPrintf("server %d find higher term %d,so become Follower in term %d\n",
								rf.me, reply.Term, rf.currentTerm)
							rf.persist()
							return
						}
						// 如果心跳成功，则更新nextIndex和matchIndex
						if reply.Success {
							// 下面这样的写话会有问题，由于网络不稳定，当过期的rpc心跳回复时，会导致nextIndex[server]被多次更新
							// rf.nextIndex[server] += len(args.Entries)
							rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[server] = rf.nextIndex[server] - 1
							// 找到一个N，使得大多数的matchIndex[i] >= N，并且log[N].term == currentTerm
							// 其实就是中位数，这里使用O(nlogN)的解法，即排序，中间的那个就是答案N/2
							sortedMatch := make([]int, len(rf.peers))
							copy(sortedMatch, rf.matchIndex)
							slices.Sort(sortedMatch)
							N := sortedMatch[len(rf.peers)/2]
							if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
								DPrintf("server %d update commitIndex from %d to %d\n", rf.me, rf.commitIndex, N)
								rf.commitIndex = N
							}
						} else {
							// 如果心跳失败，则将nextIndex减1，重新发送心跳
							// 如果优化的话，则不是将nextIndex减1，将回退一个term，即找到第一个term不同的地方
							// 证明的话看学生指南
							prevIndex := args.PrevLogIndex
							for prevIndex > 0 && rf.log[prevIndex].Term == args.PrevLogTerm {
								prevIndex--
							}
							rf.nextIndex[server] = prevIndex + 1
						}
					}
				}(i, &args)
			}
		}()
	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond)
		// Your code here (2A)
		// Check if a leader election should be started.
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 超时，本节点开启选举(Leader不用开始选举)
			if rf.state != Leader && time.Now().After(rf.electionTime) {
				// 更新选举超时时间
				rf.resetElectionTime()
				// 本节点变为候选人
				rf.state = Candidate
				// 将任期加1
				rf.currentTerm++
				// 投票给自己
				rf.votedFor = rf.me
				rf.persist()
				DPrintf("server %d start election, from Follower to Candidate in term %d\n", rf.me, rf.currentTerm)
				lastLogIndex := len(rf.log)
				lastLogTerm := 0
				if lastLogIndex > 0 {
					lastLogTerm = rf.log[lastLogIndex-1].Term
				}
				// rpc调用过程中，释放掉锁
				rf.mu.Unlock()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				// 收到投票的数量
				voteNum := 1
				// 完成应答的数量
				finishNum := 1
				// 使用通道保存应答的结果
				voteResult := make(chan *RequestVoteReply, len(rf.peers)-1)
				// 向其他节点发送投票请求
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(server int) {
						reply := RequestVoteReply{}
						if ok := rf.sendRequestVote(server, &args, &reply); ok {
							voteResult <- &reply
						} else {
							voteResult <- nil
						}
					}(i)
				}
				// 统计投票结果
				otherTerm := 0
				done := false
				for !done {
					select {
					case reply := <-voteResult:
						// 更新收到的结果
						finishNum += 1
						if reply != nil {
							if reply.VoteGranted {
								voteNum += 1
							}
							otherTerm = max(otherTerm, reply.Term)
						}
						// 如果全部应答或者已经获得大多数投票，则结束
						if finishNum == len(rf.peers) || voteNum > len(rf.peers)/2 {
							// 退出循环
							done = true
						}
					}
				}
				rf.mu.Lock()
				// 如果当前节点已经不是Candidate，则说明有其他节点已经成为Leader
				if rf.state != Candidate {
					return
				} else if rf.currentTerm < otherTerm {
					// 如果收到的投票中有更高的任期，则转变为Follower
					rf.state = Follower
					rf.currentTerm = otherTerm
					rf.votedFor = -1
					rf.persist()
					DPrintf("server %d become Follower in term %d\n", rf.me, rf.currentTerm)
				} else if voteNum > len(rf.peers)/2 {
					// 如果获得大多数投票，则转变为Leader
					rf.state = Leader
					// 初始化nextIndex和matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					DPrintf("server %d become Leader in term %d\n", rf.me, rf.currentTerm)
				}
			}
		}()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	// 论文中index从1开始，为了方便，这里添加一个空的日志
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.resetElectionTime()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// 启动心跳协程
	go rf.appendEntriesTicker()
	// 启动日志应用协程
	go rf.applyLog(applyCh)
	return rf
}
func (rf *Raft) applyLog(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		var appliedMessage = make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for rf.lastApplied < rf.commitIndex {
				// 因为lastApplied初始化为0，所以这里需要先加1，再赋值
				rf.lastApplied++
				appliedMessage = append(appliedMessage, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				})
				DPrintf("server %d apply log in term %d, index %d\n", rf.me, rf.currentTerm, rf.lastApplied)
			}
		}()
		for _, msg := range appliedMessage {
			applyCh <- msg
		}
	}
}
func (rf *Raft) resetElectionTime() {
	// 下一次超时在300-500ms之后
	ms := 300 + (rand.Int63() % 200)
	rf.electionTime = time.Now().Add(time.Millisecond * time.Duration(ms))
}
