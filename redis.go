package redis

func Startup(host string) {
	Pool = &IoRedis{
		conn: RedisPool(host),
	}
}

