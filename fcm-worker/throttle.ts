import type { Redis } from 'ioredis';

export const DURATION_LIMITER_LUA = `
local function reset()
    redis.call('HMSET', KEYS[1], 'start', ARGV[2], 'end', ARGV[2] + ARGV[3], 'count', 1)
    return redis.call('EXPIRE', KEYS[1], ARGV[3] * 2)
end

if redis.call('EXISTS', KEYS[1]) == 0 then
    return {reset(), ARGV[2] + ARGV[3], ARGV[4] - 1}
end

if ARGV[1] >= redis.call('HGET', KEYS[1], 'start') and ARGV[1] <= redis.call('HGET', KEYS[1], 'end') then
    return {
        tonumber(redis.call('HINCRBY', KEYS[1], 'count', 1)) <= tonumber(ARGV[4]),
        redis.call('HGET', KEYS[1], 'end'),
        ARGV[4] - redis.call('HGET', KEYS[1], 'count')
    }
end

return {reset(), ARGV[2] + ARGV[3], ARGV[4] - 1}
` as const;

export interface DurationAcquireResult {
    allowed: boolean;
    decaysAt: number;
    remaining: number;
}

export async function durationAcquire(
    redis: Redis,
    name: string,
    maxLocks: number,
    decay: number,
): Promise<DurationAcquireResult> {
    const nowSeconds = Date.now() / 1000;
    const nowIntSeconds = Math.floor(nowSeconds);

    const res = (await redis.eval(DURATION_LIMITER_LUA, 1, name, nowSeconds, nowIntSeconds, decay, maxLocks)) as [
        number | string | boolean,
        number | string,
        number | string,
    ];

    const allowed = !!res[0];
    const decaysAt = Number(res[1]);
    const remaining = Math.max(0, Number(res[2]));

    return { allowed, decaysAt, remaining };
}

export class DurationLimiterBuilder {
    private readonly redis: Redis;
    private readonly name: string;

    public maxLocks = 1;
    public decay = 60;
    public timeout = 3;
    public sleepMs = 750;

    constructor(redis: Redis, name: string) {
        this.redis = redis;
        this.name = name;
    }

    allow(maxLocks: number): this {
        this.maxLocks = maxLocks;
        return this;
    }

    every(decaySeconds: number): this {
        this.decay = decaySeconds;
        return this;
    }

    block(timeoutSeconds: number): this {
        this.timeout = timeoutSeconds;
        return this;
    }

    sleep(ms: number): this {
        this.sleepMs = ms;
        return this;
    }

    async then<T = any>(
        callback: () => Promise<T> | T,
        failure?: (info: DurationAcquireResult) => Promise<T | boolean> | T | boolean,
    ): Promise<T | boolean> {
        const start = Date.now();

        const tryOnce = async (): Promise<{ ok: boolean; info: DurationAcquireResult }> => {
            const info = await durationAcquire(this.redis, this.name, this.maxLocks, this.decay);

            if (info.allowed) {
                return { ok: true, info };
            }

            return { ok: false, info };
        };

        if (this.timeout <= 0) {
            const { ok, info } = await tryOnce();

            if (ok) {
                return callback();
            }

            if (failure) {
                return failure(info);
            }

            return false;
        }

        while (true) {
            const { ok, info } = await tryOnce();

            if (ok) {
                return callback();
            }

            const elapsedSeconds = (Date.now() - start) / 1000;

            if (elapsedSeconds >= this.timeout) {
                if (failure) {
                    return failure(info);
                }

                const err = new Error('LimiterTimeoutException');
                err.name = 'LimiterTimeoutException';
                throw err;
            }

            await new Promise<void>((resolve) => setTimeout(resolve, this.sleepMs));
        }
    }
}

export function throttle(redis: Redis, name: string): DurationLimiterBuilder {
    return new DurationLimiterBuilder(redis, name);
}
