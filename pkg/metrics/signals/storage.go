package signals

const (
	redisStorageReadCmdSel      = `,cmd=~"get|mget|hget|hmget"`
	redisStorageWriteCmdSel     = `,cmd=~"set|del|mset|hset|hdel"`
	redisStorageReadLatencySel  = `,cmd=~"get|mget"`
	redisStorageWriteLatencySel = `,cmd=~"set|del"`
	redisStorageReadCmdZSetSel  = `,cmd=~"zrevrange"`
	redisStorageWriteCmdZSetSel = `,cmd=~"zadd|zremrangebyrank"`
)

const (
	timelineEnginePostgres = `,engine="postgresql"`
	timelineEngineRedis    = `,engine="redis"`
)
