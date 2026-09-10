package rabbitmq_topic

func javaHash(s string) int32 {
	var hash int32 = 0
	for _, c := range s {
		hash = 31*hash + int32(c)
	}
	return hash
}

func HashShardByCount(key string, shardCount int) int {
	if shardCount <= 0 {
		panic("shardCount must be > 0")
	}
	hash := javaHash(key)
	return int((int32(hash) & 0x7fffffff) % int32(shardCount))
}
