import type { Redis } from 'ioredis';

export const DURATION_LIMITER_LUA = `
-- KEYS[1]  : limiter key
-- ARGV[1]  : nowSeconds (float)
-- ARGV[2]  : nowIntSeconds (integer, window start)
-- ARGV[3]  : decay (window size in seconds)
-- ARGV[4]  : maxLocks (max allowed operations per window)

local key        = KEYS[1]
local now        = tonumber(ARGV[1])
local now_start  = tonumber(ARGV[2])
local decay      = tonumber(ARGV[3])
local max_locks  = tonumber(ARGV[4])

local function reset()
  -- Start a new window [now_start, now_start + decay]
  local window_start = now_start
  local window_end   = now_start + decay

  redis.call('HSET',
    key,
    'start', window_start,
    'end',   window_end,
    'count', 1
  )

  -- Keep the key alive for 2x the window size
  redis.call('EXPIRE', key, decay * 2)

  local remaining = max_locks - 1
  if remaining < 0 then
    remaining = 0
  end

  -- Return: { allowed, decaysAt, remaining }
  return { true, window_end, remaining }
end

-- If the limiter key does not exist, initialize a new window
if redis.call('EXISTS', key) == 0 then
  return reset()
end

-- Key exists: read current window
local current_start = tonumber(redis.call('HGET', key, 'start'))
local current_end   = tonumber(redis.call('HGET', key, 'end'))

-- If we are still inside the current window
if now >= current_start and now <= current_end then
  -- Same as original logic: increment count with HINCRBY
  local new_count = tonumber(redis.call('HINCRBY', key, 'count', 1))

  local allowed   = new_count <= max_locks
  local remaining = max_locks - new_count
  if remaining < 0 then
    remaining = 0
  end

  -- Return: { allowed, decaysAt, remaining }
  return { allowed, current_end, remaining }
end

-- Window expired: start a new window
return reset()
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

    // ARGV mapping:
    // 1: nowSeconds
    // 2: nowIntSeconds
    // 3: decay
    // 4: maxLocks
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

    /**
     * Set the maximum number of allowed operations in a time window.
     */
    allow(maxLocks: number): this {
        this.maxLocks = maxLocks;
        return this;
    }

    /**
     * Set the window size in seconds.
     */
    every(decaySeconds: number): this {
        this.decay = decaySeconds;
        return this;
    }

    /**
     * Set the maximum time (in seconds) to wait for a slot.
     * If timeout <= 0, it will only try once without waiting.
     */
    block(timeoutSeconds: number): this {
        this.timeout = timeoutSeconds;
        return this;
    }

    /**
     * Set the sleep duration (in milliseconds) between retries
     * when waiting for a slot.
     */
    sleep(ms: number): this {
        this.sleepMs = ms;
        return this;
    }

    /**
     * Run the limiter and execute the callback when allowed.
     * If not allowed:
     *  - if `failure` is provided, it will be called with the limiter info
     *  - otherwise it throws (when timeout > 0) or returns false (when timeout <= 0)
     */
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

        // No waiting: try only once
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

        // With timeout: retry until allowed or timeout exceeded
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

/**
 * Helper to create a DurationLimiterBuilder with a given Redis client and key name.
 */
export function throttle(redis: Redis, name: string): DurationLimiterBuilder {
    return new DurationLimiterBuilder(redis, name);
}
