import Redis from 'ioredis';
import { throttle } from '../../throttle';

jest.setTimeout(30000);

describe('Throttle integration', () => {
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

    it('throttles an async service function per key in a real flow', async () => {
        const serviceCall = jest.fn(async (id: string) => {
            return `ok-${id}`;
        });

        const maxLocks = 2;
        const decay = 60;
        const key = 'integration:service:user1';

        const callServiceThroughLimiter = (requestId: string) =>
            throttle(redis, key)
                .allow(maxLocks)
                .every(decay)
                .block(0)
                .then(
                    () => serviceCall(requestId),
                    () => `throttled-${requestId}`,
                );

        const [r1, r2, r3] = await Promise.all([
            callServiceThroughLimiter('req1'),
            callServiceThroughLimiter('req2'),
            callServiceThroughLimiter('req3'),
        ]);

        expect(serviceCall).toHaveBeenCalledTimes(2);

        const results = [r1, r2, r3];

        expect(results.filter((r) => typeof r === 'string' && (r as string).startsWith('ok-')).length).toBe(2);
        expect(results.filter((r) => typeof r === 'string' && (r as string).startsWith('throttled-')).length).toBe(1);
    });

    it('applies limits independently per user key in a multi-tenant scenario', async () => {
        const serviceCall = jest.fn(async (userId: string, requestId: string) => {
            return `ok-${userId}-${requestId}`;
        });

        const maxLocksPerUser = 2;
        const decay = 60;

        const callForUser = (userId: string, requestId: string) => {
            const key = `integration:service:${userId}`;

            return throttle(redis, key)
                .allow(maxLocksPerUser)
                .every(decay)
                .block(0)
                .then(
                    () => serviceCall(userId, requestId),
                    () => `throttled-${userId}-${requestId}`,
                );
        };

        const [u1r1, u1r2, u1r3, u2r1, u2r2, u2r3] = await Promise.all([
            callForUser('user1', 'req1'),
            callForUser('user1', 'req2'),
            callForUser('user1', 'req3'),
            callForUser('user2', 'req1'),
            callForUser('user2', 'req2'),
            callForUser('user2', 'req3'),
        ]);

        const results = [u1r1, u1r2, u1r3, u2r1, u2r2, u2r3];

        expect(serviceCall).toHaveBeenCalledTimes(4);

        const resultsUser1 = results.filter((r) => (r as string).includes('user1-'));
        const resultsUser2 = results.filter((r) => (r as string).includes('user2-'));

        expect(resultsUser1.filter((r) => (r as string).startsWith('ok-')).length).toBe(2);
        expect(resultsUser1.filter((r) => (r as string).startsWith('throttled-')).length).toBe(1);

        expect(resultsUser2.filter((r) => (r as string).startsWith('ok-')).length).toBe(2);
        expect(resultsUser2.filter((r) => (r as string).startsWith('throttled-')).length).toBe(1);
    });

    it('eventually runs the service after waiting for a new window in blocking mode', async () => {
        const serviceCall = jest.fn(async () => 'ok-after-blocking-wait');

        const maxLocks = 1;
        const decay = 1;
        const key = 'integration:blocking';

        await throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(0)
            .then(async () => {
                return 'first-call';
            });

        const start = Date.now();

        const result = await throttle(redis, key)
            .allow(maxLocks)
            .every(decay)
            .block(3)
            .sleep(100)
            .then(
                () => serviceCall(),
                () => 'should-not-hit-failure',
            );

        const elapsedMs = Date.now() - start;

        expect(serviceCall).toHaveBeenCalledTimes(1);
        expect(result).toBe('ok-after-blocking-wait');

        expect(elapsedMs).toBeGreaterThanOrEqual(50);
    });
});
