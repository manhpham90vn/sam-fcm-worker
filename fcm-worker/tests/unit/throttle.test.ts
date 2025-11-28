import Redis from 'ioredis';
import { durationAcquire, throttle, DurationAcquireResult } from '../../throttle';

jest.setTimeout(30000);

describe('durationAcquire', () => {
    let redis: Redis;

    beforeAll(async () => {
        // Explicit Redis connection with short connect timeout
        redis = new Redis({
            host: '127.0.0.1',
            port: 6379,
            connectTimeout: 2000,
        });

        // Fail fast if cannot connect
        await redis.ping();
    });

    afterAll(async () => {
        await redis.quit();
    });

    beforeEach(async () => {
        await redis.flushall();
    });

    it('allows the first acquire in a fresh window and returns correct remaining', async () => {
        const maxLocks = 5;
        const decay = 60;

        const result = await durationAcquire(redis, 'test_limiter_first', maxLocks, decay);

        expect(result.allowed).toBe(true);
        expect(result.remaining).toBe(maxLocks - 1);

        const nowInt = Math.floor(Date.now() / 1000);
        expect(result.decaysAt).toBeGreaterThanOrEqual(nowInt + decay - 1);
        expect(result.decaysAt).toBeLessThanOrEqual(nowInt + decay + 1);
    });

    it('allows up to maxLocks within a single window, then blocks', async () => {
        const maxLocks = 3;
        const decay = 30;
        const key = 'test_limiter_window';

        const results: DurationAcquireResult[] = [];

        for (let i = 0; i < maxLocks; i++) {
            const res = await durationAcquire(redis, key, maxLocks, decay);
            results.push(res);
            expect(res.allowed).toBe(true);
        }

        const blocked = await durationAcquire(redis, key, maxLocks, decay);
        expect(blocked.allowed).toBe(false);
        expect(blocked.remaining).toBe(0);
        expect(blocked.decaysAt).toBe(results[0].decaysAt);
    });

    it('starts a new window after decay time has passed', async () => {
        const realNow = Date.now;
        const baseTime = realNow();

        // Mock Date.now manually instead of using fake timers
        // First window
        Date.now = () => baseTime;

        const maxLocks = 2;
        const decay = 2; // small for fast test
        const key = 'test_limiter_decay';

        const first1 = await durationAcquire(redis, key, maxLocks, decay);
        const first2 = await durationAcquire(redis, key, maxLocks, decay);
        const blocked = await durationAcquire(redis, key, maxLocks, decay);

        expect(first1.allowed).toBe(true);
        expect(first2.allowed).toBe(true);
        expect(blocked.allowed).toBe(false);

        // Move "time" forward beyond the decay window
        Date.now = () => baseTime + (decay + 1) * 1000;

        const afterWindow = await durationAcquire(redis, key, maxLocks, decay);
        expect(afterWindow.allowed).toBe(true);
        expect(afterWindow.remaining).toBe(maxLocks - 1);

        // Restore original Date.now
        Date.now = realNow;
    });

    it('with maxLocks = 0 it allows only the first call and blocks subsequent ones', async () => {
        const maxLocks = 0;
        const decay = 10;
        const key = 'test_limiter_zero';

        const res1 = await durationAcquire(redis, key, maxLocks, decay);
        const res2 = await durationAcquire(redis, key, maxLocks, decay);

        // First acquire: allowed (because reset() always returns allowed = true)
        expect(res1.allowed).toBe(true);
        expect(res1.remaining).toBe(0); // maxLocks - 1 clamped to 0

        // Second acquire in same window: blocked
        expect(res2.allowed).toBe(false);
        expect(res2.remaining).toBe(0);
        expect(res2.decaysAt).toBe(res1.decaysAt);
    });

    it('never returns negative remaining even if called many times', async () => {
        const maxLocks = 3;
        const decay = 30;
        const key = 'test_limiter_remaining_non_negative';

        const calls = 10;
        let last: DurationAcquireResult = { allowed: false, decaysAt: 0, remaining: 0 };

        for (let i = 0; i < calls; i++) {
            last = await durationAcquire(redis, key, maxLocks, decay);
            expect(last.remaining).toBeGreaterThanOrEqual(0);
        }

        expect(last.allowed).toBe(false);
        expect(last.remaining).toBe(0);
    });

    it('sets TTL close to decay * 2 on first acquire', async () => {
        const maxLocks = 2;
        const decay = 10;
        const key = 'test_limiter_ttl';

        await durationAcquire(redis, key, maxLocks, decay);

        const ttl = await redis.ttl(key);
        // TTL should be around decay * 2, but allow some slack for execution time
        expect(ttl).toBeGreaterThanOrEqual(decay * 2 - 2);
        expect(ttl).toBeLessThanOrEqual(decay * 2 + 2);
    });

    it('only one of two concurrent acquires with maxLocks=1 is allowed', async () => {
        const maxLocks = 1;
        const decay = 30;
        const key = 'test_limiter_concurrent';

        const [r1, r2] = await Promise.all([
            durationAcquire(redis, key, maxLocks, decay),
            durationAcquire(redis, key, maxLocks, decay),
        ]);

        const allowedCount = [r1, r2].filter((r) => r.allowed).length;
        const blockedCount = [r1, r2].filter((r) => !r.allowed).length;

        expect(allowedCount).toBe(1);
        expect(blockedCount).toBe(1);
    });

    it('maintains independent windows and counters per key', async () => {
        const maxLocks = 2;
        const decay = 60;

        const keyA = 'test_limiter_multi_key_A';
        const keyB = 'test_limiter_multi_key_B';

        const a1 = await durationAcquire(redis, keyA, maxLocks, decay);
        const a2 = await durationAcquire(redis, keyA, maxLocks, decay);
        const a3 = await durationAcquire(redis, keyA, maxLocks, decay);

        expect(a1.allowed).toBe(true);
        expect(a2.allowed).toBe(true);
        expect(a3.allowed).toBe(false);

        const b1 = await durationAcquire(redis, keyB, maxLocks, decay);
        const b2 = await durationAcquire(redis, keyB, maxLocks, decay);
        const b3 = await durationAcquire(redis, keyB, maxLocks, decay);

        expect(b1.allowed).toBe(true);
        expect(b2.allowed).toBe(true);
        expect(b3.allowed).toBe(false);

        expect(a1.decaysAt).toBe(a2.decaysAt);
        expect(b1.decaysAt).toBe(b2.decaysAt);
    });

    it('treats now == window end as inside the current window (boundary test)', async () => {
        const realNow = Date.now;

        const alignedBaseSec = 2_000_000_000;
        const alignedBaseMs = alignedBaseSec * 1000;

        const maxLocks = 1;
        const decay = 5;
        const key = 'test_limiter_boundary_inclusive_end';

        Date.now = () => alignedBaseMs;

        const first = await durationAcquire(redis, key, maxLocks, decay);
        expect(first.allowed).toBe(true);

        // Window start = nowIntSeconds = alignedBaseSec
        const windowStartSec = alignedBaseSec;
        const windowEndSec = windowStartSec + decay;

        Date.now = () => windowEndSec * 1000;

        const atBoundary = await durationAcquire(redis, key, maxLocks, decay);

        expect(atBoundary.allowed).toBe(false);
        expect(atBoundary.decaysAt).toBe(first.decaysAt);

        Date.now = realNow;
    });
});

describe('DurationLimiterBuilder / throttle helper', () => {
    let redis: Redis;

    beforeAll(async () => {
        redis = new Redis({
            host: '127.0.0.1',
            port: 6379,
            connectTimeout: 2000,
        });
        await redis.ping();
    });

    afterAll(async () => {
        await redis.quit();
    });

    beforeEach(async () => {
        await redis.flushall();
    });

    it('executes callback when allowed and does not call failure', async () => {
        const key = 'builder_allowed';
        const callback = jest.fn().mockResolvedValue('ok');
        const failure = jest.fn();

        const result = await throttle(redis, key).allow(1).every(60).block(0).then(callback, failure);

        expect(callback).toHaveBeenCalledTimes(1);
        expect(failure).not.toHaveBeenCalled();
        expect(result).toBe('ok');
    });

    it('calls failure when not allowed and timeout is 0', async () => {
        const key = 'builder_blocked';
        const callback = jest.fn();
        const failure = jest.fn().mockResolvedValue('failed');

        // First call consumes the only slot
        await throttle(redis, key)
            .allow(1)
            .every(60)
            .block(0)
            .then(async () => undefined);

        // Second call should be throttled immediately
        const result = await throttle(redis, key).allow(1).every(60).block(0).then(callback, failure);

        expect(callback).not.toHaveBeenCalled();
        expect(failure).toHaveBeenCalledTimes(1);
        expect(result).toBe('failed');
    });

    it('returns false when not allowed, timeout is 0 and failure is not provided', async () => {
        const key = 'builder_blocked_no_failure';
        const callback = jest.fn();

        // Consume slot
        await throttle(redis, key)
            .allow(1)
            .every(60)
            .block(0)
            .then(async () => undefined);

        const result = await throttle(redis, key).allow(1).every(60).block(0).then(callback);

        expect(callback).not.toHaveBeenCalled();
        expect(result).toBe(false);
    });

    it('waits until a slot is available when timeout > 0', async () => {
        const realNow = Date.now;
        const baseTime = realNow();

        const key = 'builder_wait';
        const callback = jest.fn().mockResolvedValue('ok-after-wait');
        const failure = jest.fn();

        const maxLocks = 1;
        const decay = 2;

        // First call consumes the only slot in the window
        Date.now = () => baseTime;
        await throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(0)
            .then(async () => 'first');

        // Second call: simulate time passing so limiter sees a new window
        Date.now = () => baseTime + (decay + 1) * 1000;

        const result = await throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(5) // can wait up to 5 seconds, but our Date.now mock makes it pass immediately
            .then(callback, failure);

        expect(callback).toHaveBeenCalledTimes(1);
        expect(failure).not.toHaveBeenCalled();
        expect(result).toBe('ok-after-wait');

        Date.now = realNow;
    });

    it('calls failure when timeout > 0 and slot never becomes available', async () => {
        const key = 'builder_timeout_with_failure';
        const callback = jest.fn();
        const failure = jest.fn().mockResolvedValue('timeout-failed');

        const maxLocks = 1;
        const decay = 100; // long window

        // Consume the only slot
        await throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(0)
            .then(async () => 'first');

        // Second call: window is still active and we keep Date.now as-is
        const result = await throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(1) // small timeout
            .sleep(0) // do not actually sleep, loop fast
            .then(callback, failure);

        expect(callback).not.toHaveBeenCalled();
        expect(failure).toHaveBeenCalledTimes(1);
        expect(result).toBe('timeout-failed');
    });

    it('throws LimiterTimeoutException when timeout > 0, no failure, and slot never becomes available', async () => {
        const key = 'builder_timeout_throw';
        const callback = jest.fn();

        const maxLocks = 1;
        const decay = 100;

        // Consume the only slot
        await throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(0)
            .then(async () => 'first');

        const promise = throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(1) // small timeout
            .sleep(0) // do not actually sleep, loop fast
            .then(callback);

        await expect(promise).rejects.toMatchObject({
            name: 'LimiterTimeoutException',
            message: 'LimiterTimeoutException',
        });

        expect(callback).not.toHaveBeenCalled();
    });

    it('propagates boolean result from failure handler when timeout = 0', async () => {
        const key = 'builder_failure_boolean';
        const callback = jest.fn();
        const failure = jest.fn().mockResolvedValue(false);

        await throttle(redis, key)
            .allow(1)
            .every(60)
            .block(0)
            .then(async () => 'first');

        const result = await throttle(redis, key).allow(1).every(60).block(0).then(callback, failure);

        expect(callback).not.toHaveBeenCalled();
        expect(failure).toHaveBeenCalledTimes(1);
        expect(result).toBe(false);
    });

    it('only runs one callback when two builder calls compete concurrently with maxLocks=1', async () => {
        const key = 'builder_concurrent';
        const callback1 = jest.fn().mockResolvedValue('cb1');
        const callback2 = jest.fn().mockResolvedValue('cb2');
        const failure1 = jest.fn().mockResolvedValue('f1');
        const failure2 = jest.fn().mockResolvedValue('f2');

        const [r1, r2] = await Promise.all([
            throttle(redis, key).allow(1).every(60).block(0).then(callback1, failure1),
            throttle(redis, key).allow(1).every(60).block(0).then(callback2, failure2),
        ]);

        const callbacksCalled = [callback1, callback2].filter((fn) => fn.mock.calls.length > 0).length;
        const failuresCalled = [failure1, failure2].filter((fn) => fn.mock.calls.length > 0).length;

        expect(callbacksCalled).toBe(1);
        expect(failuresCalled).toBe(1);

        expect(['cb1', 'cb2', 'f1', 'f2']).toContain(r1);
        expect(['cb1', 'cb2', 'f1', 'f2']).toContain(r2);
    });
});
