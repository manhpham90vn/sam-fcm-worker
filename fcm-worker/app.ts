import { SQSEvent, SQSRecord, Context } from 'aws-lambda';
import { throttle } from './throttle';
import Redis, { RedisOptions } from 'ioredis';

let client: Redis;

const MAX_BATCHES_PER_MINUTE = 1200;
const WINDOW_SECONDS = 60;
const FCM_THROTTLE_KEY = 'fcm_throttle_key';
const MAX_RECEIVE_COUNT = 5;

function getRedis() {
    if (!client) {
        const url = process.env.REDIS_URL;
        if (!url) {
            throw new Error('REDIS_URL is not set');
        }
        const redisUrl = new URL(url);
        const options: RedisOptions = {
            host: redisUrl.hostname,
            port: Number(redisUrl.port) || 6379,
            password: redisUrl.password || undefined,
            db: redisUrl.pathname ? Number(redisUrl.pathname.slice(1)) : 0,
            maxRetriesPerRequest: 3,
            retryStrategy(times) {
                if (times >= 3) {
                    return null;
                }
                return times * 200;
            },
            enableOfflineQueue: false,
        };
        client = new Redis(options);
    }
    return client;
}

export const lambdaHandler = async (event: SQSEvent, context: Context): Promise<void> => {
    console.log('Received event:', JSON.stringify(event));

    let redis: Redis;

    try {
        redis = getRedis();
    } catch (err) {
        console.error('Failed to init Redis client, STOP', err);
        return;
    }

    for (const record of event.Records as SQSRecord[]) {
        console.log('Raw SQS body:', record.body);

        const receiveCount = Number(record.attributes?.ApproximateReceiveCount ?? '1');

        if (receiveCount > MAX_RECEIVE_COUNT) {
            console.log(`Message exceeded max receive count (${MAX_RECEIVE_COUNT}) STOP`);
            return;
        }

        try {
            const payload = JSON.parse(record.body);
            const { type, title, body, topic, tokens } = payload;
            await throttle(redis, FCM_THROTTLE_KEY)
                .allow(MAX_BATCHES_PER_MINUTE)
                .every(WINDOW_SECONDS)
                .block(0)
                .then(
                    async () => {
                        if (type === 'topic') {
                            console.log('Pushing topic', title, body, topic);
                        } else if (type === 'tokens') {
                            console.log('Pushing tokens', title, body, tokens);
                        }
                    },
                    async () => {
                        console.log('Throttled by limiter');
                        throw new Error('fcm_throttled');
                    },
                );
        } catch (err) {
            console.error('Error handling SQS message:', err);

            if (err instanceof SyntaxError) {
                console.warn('Invalid JSON, skipping message STOP');
                return;
            }

            throw err;
        }
    }
};
