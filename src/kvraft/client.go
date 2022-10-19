package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId   int64
	commandId uint64
	leaderId  int
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
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.commandId = uint64(1)
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	clerkId := ck.clerkId
	commandId := ck.commandId
	leaderId := ck.leaderId
	ck.commandId++
	ck.mu.Unlock()

	args := GetArgs{
		Key:       key,
		ClerkId:   clerkId,
		CommandId: commandId,
	}
	reply := GetReply{}
	//defer func() {
	//	DPrintf("{Clerk %v} send Get request args %v and reply %v", ck.clerkId, args, reply)
	//}()

	//for {
	//	ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
	//	DPrintf("{Clerk %v} send Get request, ok %v args %v and reply %v", ck.clerkId, ok, &args, &reply)
	//	for nSend := 1; !ok && nSend < 5; nSend++ {
	//		time.Sleep(100 * time.Millisecond)
	//		ok = ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
	//		DPrintf("{Clerk %v} send Get request, ok %v args %v and reply %v", ck.clerkId, ok, &args, &reply)
	//	}
	//	// !ok : leaderId++
	//	// reply.Err == ErrDoneCommandId : 换新commandId + 不变leaderId
	//	// reply.Err == ErrWrongLeader : leaderId++
	//	// else: break
	//	if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
	//		leaderId = (leaderId + 1) % len(ck.servers)
	//	} else if reply.Err == ErrDoneCommandId {
	//		ck.mu.Lock()
	//		args.CommandId = ck.commandId
	//		ck.commandId++
	//		ck.mu.Unlock()
	//	} else {
	//		break
	//	}
	//	time.Sleep(50 * time.Millisecond)
	//}

	for {
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		DPrintf("{Clerk %v} send Get request, ok %v args %v and reply %v", ck.clerkId, ok, &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		} else if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}
		ck.mu.Lock()
		args.CommandId = ck.commandId
		ck.commandId++
		ck.mu.Unlock()
	}

	ck.mu.Lock()
	if ck.leaderId != leaderId {
		ck.leaderId = leaderId
	}
	ck.mu.Unlock()
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	clerkId := ck.clerkId
	commandId := ck.commandId
	leaderId := ck.leaderId
	ck.commandId++
	ck.mu.Unlock()

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   clerkId,
		CommandId: commandId,
	}
	reply := PutAppendReply{}

	//for {
	//	ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
	//	DPrintf("{Clerk %v} send PutAppend request, ok %v args %v and reply %v", ck.clerkId, ok, &args, &reply)
	//	for nSend := 1; !ok && nSend < 5; nSend++ {
	//		time.Sleep(100 * time.Millisecond)
	//		ok = ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
	//		DPrintf("{Clerk %v} send PutAppend request, ok %v args %v and reply %v", ck.clerkId, ok, &args, &reply)
	//	}
	//	// !ok : leaderId++
	//	// reply.Err == ErrDoneCommandId : 换新commandId + 不变leaderId
	//	// reply.Err == ErrWrongLeader : leaderId++
	//	// else: break
	//	if !ok || reply.Err == ErrWrongLeader {
	//		leaderId = (leaderId + 1) % len(ck.servers)
	//	} else if reply.Err == ErrDoneCommandId {
	//		ck.mu.Lock()
	//		args.CommandId = ck.commandId
	//		ck.commandId++
	//		ck.mu.Unlock()
	//	} else {
	//		break
	//	}
	//	time.Sleep(50 * time.Millisecond)
	//}

	for {
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("{Clerk %v} send PutAppend request, ok %v args %v and reply %v", ck.clerkId, ok, &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		} else if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}
		ck.mu.Lock()
		args.CommandId = ck.commandId
		ck.commandId++
		ck.mu.Unlock()
	}

	ck.mu.Lock()
	if ck.leaderId != leaderId {
		ck.leaderId = leaderId
	}
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
