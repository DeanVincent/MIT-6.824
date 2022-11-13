package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkId  int64
	cmdId    int64
	leaderId int
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
	// Your code here.
	ck.clerkId = nrand()
	ck.cmdId = 1
	return ck
}

func (ck *Clerk) cmd(args *CmdArgs) Config {
	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			var reply CmdReply
			ok := ck.servers[ck.leaderId].Call("ShardCtrler.Cmd", args, &reply)
			DPrintf("{Clerk %v} send %v request to {Node %v}, ok %v args %v and reply %v",
				ck.clerkId, args.Type.toString(), ck.leaderId, ok, args, reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		//for _, srv := range ck.servers {
		//	var reply CmdReply
		//	ok := srv.Call("ShardCtrler.Cmd", args, &reply)
		//	DPrintf("{Clerk %v} send %v request, ok %v args %v and reply %v",
		//		ck.clerkId, args.Type.toString(), ok, args, reply)
		//	if ok && reply.Err == OK {
		//		return reply.Config
		//	}
		//}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Query(num int) Config {
	args := CmdArgs{
		Type:    CmdQuery,
		ClerkId: ck.clerkId,
		CmdId:   ck.cmdId,
		Num:     num,
	}
	ck.cmdId++
	return ck.cmd(&args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := CmdArgs{
		Type:    CmdJoin,
		ClerkId: ck.clerkId,
		CmdId:   ck.cmdId,
		Servers: servers,
	}
	ck.cmdId++
	ck.cmd(&args)
}

func (ck *Clerk) Leave(gids []int) {
	args := CmdArgs{
		Type:    CmdLeave,
		ClerkId: ck.clerkId,
		CmdId:   ck.cmdId,
		GIDs:    gids,
	}
	ck.cmdId++
	ck.cmd(&args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := CmdArgs{
		Type:    CmdMove,
		ClerkId: ck.clerkId,
		CmdId:   ck.cmdId,
		GIDs:    []int{gid},
		Shard:   shard,
	}
	ck.cmdId++
	ck.cmd(&args)
}

//func (ck *Clerk) Query(num int) Config {
//	args := &QueryArgs{}
//	// Your code here.
//	args.Num = num
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply QueryReply
//			ok := srv.Call("ShardCtrler.Query", args, &reply)
//			if ok && reply.Err == OK {
//				return reply.Config
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
//
//func (ck *Clerk) Join(servers map[int][]string) {
//	args := &JoinArgs{}
//	// Your code here.
//	args.Servers = servers
//
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply JoinReply
//			ok := srv.Call("ShardCtrler.Join", args, &reply)
//			if ok && reply.Err == OK {
//				return
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
//
//func (ck *Clerk) Leave(gids []int) {
//	args := &LeaveArgs{}
//	// Your code here.
//	args.GIDs = gids
//
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply LeaveReply
//			ok := srv.Call("ShardCtrler.Leave", args, &reply)
//			if ok && reply.Err == OK {
//				return
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
//
//func (ck *Clerk) Move(shard int, gid int) {
//	args := &MoveArgs{}
//	// Your code here.
//	args.Shard = shard
//	args.GID = gid
//
//	for {
//		// try each known server.
//		for _, srv := range ck.servers {
//			var reply MoveReply
//			ok := srv.Call("ShardCtrler.Move", args, &reply)
//			if ok && reply.Err == OK {
//				return
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
