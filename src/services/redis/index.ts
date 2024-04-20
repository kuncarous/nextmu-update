import * as redis from 'ioredis';

export const RedisConnection: redis.RedisOptions = {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT != null ? +process.env.REDIS_PORT : undefined,
    username: process.env.REDIS_USER || undefined,
    password: process.env.REDIS_PASS || undefined,
    tls:
        Number(process.env.REDIS_SSL || 0) >= 1
            ? { servername: process.env.REDIS_HOST }
            : undefined,
};
export const client = new redis.Redis(RedisConnection);
