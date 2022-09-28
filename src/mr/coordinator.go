// MapReduce 程序设计笔记
//
// 分布式程序的实现, 离不开三个工具:
// 1. RPC, 掩盖通信是在不可靠的网络上的事实
// 2. 线程, 利用多核心, 提供了一种结构化的并发操作方式
// 3. 并发控制, 比如锁
//
// Coordinator: channel-based, worker-stateless, high-performance
//
// 主题: Coordinator 的线程模型与并发控制
// 方案: lock-free, 使用 channel 来替代锁.
// (其实, channel 的并发控制也基于锁, 只是相对于 Coordinator 的锁来说, 这个锁的粒度更小)
// Coordinator 只有一个更新线程 core, 其他线程, 包括: RPC线程, 定时器监听线程, 会将不同的更新消息发送到相应的 channel 中, core 线程
// 监听这些 channel 并进行相应的处理.
//
// 主题: Coordinator 与 Worker 的通信类型
// 1. pullTask: Worker 向 Coordinator 要 Task 来执行
// 2. reportTask: Worker 向 Coordinator 报告 Task 的执行结果
//
// 提示: 对于一个任务执行了很久的 Worker, Coordinator 无法可靠地区分 Worker 是停止了, 还是卡住了, 还是正在执行但速度过慢而无法发挥作用.
// 我们可以让 Coordinator 等待一定的时间, 然后将 Task 重新分配给另一个 Worker. 在本程序中, 等待的时间是10秒钟.
//
// 背景: 在本程序中, 假设 Coordinator 和所有的 Worker 运行在同一节点, 共享一个文件系统
// 问题: Coordinator 是否需要记录 intermediate 和 result
// 方案: 由于所有进程共用一个文件系统, 而且文件路径与命名规则是可以固定在程序内的, 因此 Worker 不需要返回, 并且 Coordinator 不需要记录
//
// 背景: 同上
// 问题: Coordinator 是否需要维护所有 Worker 的状态
// 方案: 不需要. 在本程序中, Worker 主动向 Coordinator 请求 Task, 而不是由 Coordinator 来调度 Worker;
//      而在生产级别的实现中需要维护, 其原因是 Coordinator 主动调度 Task, 需要路由和负载均衡
//
// 背景: Worker 会通过 RPC 向 Coordinator 请求 Task 来执行, Coordinator 收到请求后, 会将未开始或者执行超时的 Task 分配给 Worker
// 问题: Coordinator 如何知道下一个分配的 Task 是哪个
// 方案一: Coordinator 在每次收到请求后遍历一遍 Task 数组, 当找到一个满足要求的 Task 时, 将其返回.
// 优点: 实现简单
// 缺点: 浪费时间, Coordinator 每次遍历的时间复杂度是 nTask
// 方案二: Coordinator 维护一个 Task 分配队列, 收到请求时直接从队列取出 Task 分配给 Worker. Coordinator 在初始化时将所有 Task 放进队列,
//        以及 Task 超时后重新放进队列. 本质上, 一次性地确定顺序可以避免多次的查询. 该想法源于 ZooKeeper 无群体效应的锁原语
// 优点: Coordinator 每次分配 Task 的时间复杂度是 1
// 缺点: 需要与 Task 超时逻辑相联动
//
// 背景: 每个 Task 的执行有时间限制, 当 Coordinator 发现一个 Task 的执行时间已超过限制时, Coordinator 将 Task 重新分配给另一个 Worker
// 问题: Coordinator 如何检测到某个 Task 超时, 以及如果安排重新执行
// 方案一: (基于 Task 分配队列) 每个 Task 记录自己的开始时间, Coordinator 在每次收到请求后遍历一遍 Task 数组, 检查执行中的 Task 是否超时,
//        如果超时则直接将其分配
// 优点: 实现简单
// 缺点: Coordinator 每次遍历时间复杂度是 nTask
// 方案二: (基于 Task 分配队列) 每个 Task 有一个计时的 ticker, 一个监听 ticker.C 的线程和一个接收完成消息的 channel. 当线程监听到超时时,
//        就将 Task 放进队列, 然后停止监听. 若在监听过程中, 收到 Task 已完成的消息, 则停止监听.
// 优点: 有专门负责监听的线程, 对 Coordinator 毫无影响
// 缺点: 监听线程的数量为 nTask, 有额外的内存占用
//
// 提示: 为了确保部分写入的文件不会被其他进程观察到, MapReduce 论文中提到了使用临时文件并在完全写入后原子化地重命名的技巧.
//
// 背景: 同一个 Task 可能会被多个 Worker 先后执行并且写入同一文件, 虽然写入操作是原子的, 但后执行完的结果会将之前的结果覆盖
// 问题: 覆盖行为是否会改变结果
// 分析: 如果 Task 是幂等的, 那么多次执行的结果都一样, 覆盖行为不会影响最终结果;
//      如果 Task 是非幂等的, 假设一个 map 被执行了两次, 一个 reduce 看到第一次运行的输出, 另一个 Reduce 看到第二次运行的输出. 当然,
//      MapReduce 需要保证正确性, 因此 Task 必须是幂等的, 只允许看到参数, 没有状态, I/O, 交互, 外部通信

package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	phase     SchedulePhase
	tasks     []Task
	files     []string
	nMap      int
	nReduce   int
	nComplete int
	assignCh  chan int
	pullCh    chan TaskPullMsg
	reportCh  chan TaskReportMsg
	doneCh    chan struct{}
}

type TaskPullMsg struct {
	reply *PullTaskReply
	ready chan struct{}
}

type TaskReportMsg struct {
	args  *ReportTaskArgs
	ready chan struct{}
}

type Task struct {
	id        int
	state     StateType
	completed chan struct{} // for Coordinator to send completed msg to inspector of this Task
	ticker    *time.Ticker  // triggerred when assigned, stopped when reported result or timeout
}

// core is the only one thread that update Coordinator
// msg channel used by core to communicate with RPC thread etc.
func (c *Coordinator) core() {
	for {
		select {
		case msg := <-c.pullCh:
			c.onPullTask(msg)
		case msg := <-c.reportCh:
			c.onReportTask(msg)
		}
	}
}

// inspector is thread that watch timeout of a task and when timeout push task to channel
func (c *Coordinator) inspector(t Task) {
	select {
	case <-t.completed:
		return
	case <-t.ticker.C:
		t.ticker.Stop()
		c.assignCh <- t.id
	}
}

// onPullTask pop a task from channel for assignment, and start inspector thread and a ticker
func (c *Coordinator) onPullTask(msg TaskPullMsg) {
	reply := msg.reply

	if c.phase == CompletePhase {
		reply.Action = CompleteAction
		msg.ready <- struct{}{}
		return
	}

	select {
	case reply.Id = <-c.assignCh:
		if c.phase == MapPhase {
			reply.Action = MapAction
			reply.MapInputFile = c.files[reply.Id]
		} else {
			reply.Action = ReduceAction
		}
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce

		c.tasks[reply.Id].state = InProgressState
		c.tasks[reply.Id].ticker = time.NewTicker(TaskTime)
		go c.inspector(c.tasks[reply.Id])
	default:
		reply.Action = WaitAction
	}

	msg.ready <- struct{}{}
}

// onReportTask update reported Task state to completed and stop its ticker
// count completed Task and update Coordinator phase when all task for current phase completed
func (c *Coordinator) onReportTask(msg TaskReportMsg) {
	args := msg.args

	if !((args.Action == MapAction && c.phase == MapPhase) || (args.Action == ReduceAction && c.phase == ReducePhase)) {
		msg.ready <- struct{}{}
		return
	}

	if c.tasks[args.Id].state != InProgressState {
		msg.ready <- struct{}{}
		return
	}

	c.tasks[args.Id].state = CompletedState
	c.tasks[args.Id].ticker.Stop()
	c.tasks[args.Id].completed <- struct{}{}
	c.nComplete++

	if c.phase == MapPhase && c.nComplete == c.nMap {
		DPrintf("Coordinator: %v finished, start %v \n", MapPhase, ReducePhase)
		c.initReducePhase()
	} else if c.phase == ReducePhase && c.nComplete == c.nReduce {
		DPrintf("Coordinator: %v finished, start %v \n", ReducePhase, CompletePhase)
		c.initCompletePhase()
	}

	msg.ready <- struct{}{}
}

func (c *Coordinator) PullTask(args *PullTaskArgs, reply *PullTaskReply) error {
	msg := TaskPullMsg{reply: reply, ready: make(chan struct{})}
	c.pullCh <- msg
	<-msg.ready
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	msg := TaskReportMsg{args: args, ready: make(chan struct{})}
	c.reportCh <- msg
	<-msg.ready
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// if the entire job has finished.
// return only once
func (c *Coordinator) Done() bool {
	<-c.doneCh
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		nMap:     len(files),
		nReduce:  nReduce,
		assignCh: make(chan int, max(len(files), nReduce)),
		pullCh:   make(chan TaskPullMsg),
		reportCh: make(chan TaskReportMsg),
		doneCh:   make(chan struct{}, 1),
	}

	c.initMapPhase()

	c.server()

	go c.core()

	return &c
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.tasks[i] = Task{
			id:        i,
			state:     IdleState,
			completed: make(chan struct{}, 1),
		}
		c.assignCh <- i
	}
	c.nComplete = 0
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:        i,
			state:     IdleState,
			completed: make(chan struct{}, 1),
		}
		c.assignCh <- i
	}
	c.nComplete = 0
}

func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}
