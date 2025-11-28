import Redis from 'ioredis';
import { durationAcquire, throttle } from '../../throttle';

jest.setTimeout(120000);

describe('Throttle perf / load tests', () => {
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

    it('benchmarks raw durationAcquire throughput with many unique keys', async () => {
        const N = 10000;
        const maxLocks = 1;
        const decay = 60;

        const start = Date.now();

        for (let i = 0; i < N; i++) {
            const key = `perf:raw:${i}`;
            await durationAcquire(redis, key, maxLocks, decay);
        }

        const elapsedMs = Date.now() - start;
        const opsPerSec = (N / elapsedMs) * 1000;

        console.log(`\n[Perf] durationAcquire: ${N} calls in ${elapsedMs} ms -> ~${opsPerSec.toFixed(2)} ops/sec`);

        expect(opsPerSec).toBeGreaterThan(500);
    });

    it('handles high concurrency with builder without errors or deadlocks', async () => {
        const concurrentClients = 50;
        const iterationsPerClient = 50;
        const maxLocksPerWindow = 5;
        const decay = 5;

        const key = 'perf:builder:concurrent';
        const serviceCall = jest.fn(async (clientId: number, iter: number) => {
            return `ok-${clientId}-${iter}`;
        });

        const failureCall = jest.fn(async (clientId: number, iter: number) => {
            return `throttled-${clientId}-${iter}`;
        });

        const clientTasks: Promise<unknown>[] = [];

        const start = Date.now();

        for (let c = 0; c < concurrentClients; c++) {
            const clientId = c;
            const task = (async () => {
                for (let i = 0; i < iterationsPerClient; i++) {
                    await throttle(redis, key)
                        .allow(maxLocksPerWindow)
                        .every(decay)
                        .block(0)
                        .then(
                            () => serviceCall(clientId, i),
                            () => failureCall(clientId, i),
                        );
                }
            })();

            clientTasks.push(task);
        }

        await Promise.all(clientTasks);

        const elapsedMs = Date.now() - start;
        const totalCalls = concurrentClients * iterationsPerClient;

        // eslint-disable-next-line no-console
        console.log(`\n[Load] builder: ${totalCalls} logical calls (callback+failure) in ${elapsedMs} ms`);

        expect(serviceCall.mock.calls.length + failureCall.mock.calls.length).toBe(totalCalls);

        expect(serviceCall.mock.calls.length).toBeGreaterThan(0);
        expect(serviceCall.mock.calls.length).toBeLessThan(totalCalls);
    });
});
