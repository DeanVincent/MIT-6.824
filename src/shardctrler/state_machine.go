package shardctrler

import "sort"

type ctrlerStateMachine interface {
	exec(cmd Cmd) (*Config, Err)
}

type memoryCtrler struct {
	me      int
	configs []*Config // indexed by config num
}

const NoGroup int = 0

func newMemoryCtrler(me int) *memoryCtrler {
	sm := &memoryCtrler{
		me:      me,
		configs: make([]*Config, 1),
	}
	sm.configs[0] = NewDefaultConfig()
	return sm
}

func NewDefaultConfig() *Config {
	return &Config{
		Num:    0,
		Groups: map[int][]string{},
	}
}

func (c *memoryCtrler) exec(cmd Cmd) (*Config, Err) {
	switch cmd.Type {
	case CmdQuery:
		return c.query(cmd.Num)
	case CmdJoin:
		return nil, c.join(cmd.Servers)
	case CmdLeave:
		return nil, c.leave(cmd.GIDs)
	case CmdMove:
		return nil, c.move(cmd.Shard, cmd.GIDs[0])
	}
	return nil, ErrUnexpectedType
}

func (c *memoryCtrler) query(num int) (*Config, Err) {
	if num == -1 || num >= len(c.configs) {
		num = len(c.configs) - 1
	}
	config := c.copyConfig(num)
	return config, OK
}

func (c *memoryCtrler) join(servers map[int][]string) Err {
	config := c.copyConfig(len(c.configs) - 1)
	config.Num++

	for gid, nodes := range servers {
		if _, has := config.Groups[gid]; has {
			DPrintf("{Node %v} memoryCtrler Error: group %v can't be added, because it has been existed, exist %v.",
				c.me, gid, config.Groups)
			continue
		}
		config.Groups[gid] = nodes
	}

	c.rebalance(config)
	c.configs = append(c.configs, config)
	DPrintf("{Node %v} memoryCtrler's state is %v, after join %v", c.me, *config, servers)
	return OK
}

func (c *memoryCtrler) leave(gids []int) Err {
	config := c.copyConfig(len(c.configs) - 1)
	config.Num++

	for _, gid := range gids {
		if _, has := config.Groups[gid]; !has {
			DPrintf("{Node %v} memoryCtrler Error: group %v can't be removed, because it doesn't exist, exist %v.",
				c.me, gid, config.Groups)
			continue
		}
		for i := 0; i < NShards; i++ {
			if config.Shards[i] == gid {
				config.Shards[i] = NoGroup
			}
		}
		delete(config.Groups, gid)
	}

	c.rebalance(config)
	c.configs = append(c.configs, config)
	DPrintf("{Node %v} memoryCtrler's state is %v, after leave %v", c.me, *config, gids)
	return OK
}

func (c *memoryCtrler) move(shard int, gid int) Err {
	config := c.copyConfig(len(c.configs) - 1)
	config.Num++

	if _, has := config.Groups[gid]; !has {
		return ErrNoGroup
	}
	config.Shards[shard] = gid

	c.configs = append(c.configs, config)
	DPrintf("{Node %v} memoryCtrler's state is %v, after move shard %v to group %v", c.me, *config, shard, gid)
	return OK
}

func (c *memoryCtrler) rebalance(config *Config) {
	m := make(map[int][]int) // gid -> []shard
	for gid, _ := range config.Groups {
		m[gid] = []int{}
	}

	noGroupShards := make(chan int, NShards) // must be a buffered channel
	for shard, gid := range config.Shards {
		if gid == NoGroup {
			noGroupShards <- shard
			continue
		}
		m[gid] = append(m[gid], shard)
	}

	var nodes []node
	for gid, shards := range m {
		nodes = append(nodes, node{gid: gid, shards: shards})
	}
	sort.Sort(byShardNum(nodes))

	nGroup := len(config.Groups)
	if nGroup == 0 {
		return
	}

	base := NShards / nGroup
	pos := NShards % nGroup
	for i, node := range nodes {
		tarLen := base
		if i < pos {
			tarLen++
		}
		for len(node.shards) < tarLen {
			shard := <-noGroupShards
			node.shards = append(node.shards, shard)
			config.Shards[shard] = node.gid
		}
		for len(node.shards) > tarLen {
			noGroupShards <- node.shards[len(node.shards)-1]
			node.shards = node.shards[:len(node.shards)-1]
		}
	}
}

type node struct {
	gid    int
	shards []int
}

type byShardNum []node

func (a byShardNum) Len() int {
	return len(a)
}

func (a byShardNum) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byShardNum) Less(i, j int) bool {
	if len(a[i].shards) == len(a[j].shards) {
		return a[i].gid < a[j].gid
	}
	return len(a[i].shards) > len(a[j].shards)
}

func (c *memoryCtrler) copyConfig(num int) *Config {
	config := new(Config)
	config.Num = num
	for i := 0; i < len(c.configs[num].Shards); i++ {
		config.Shards[i] = c.configs[num].Shards[i]
	}
	config.Groups = make(map[int][]string)
	for k, v := range c.configs[num].Groups {
		config.Groups[k] = v
	}
	return config
}
