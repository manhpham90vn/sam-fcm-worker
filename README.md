# sam-fcm-worker

## Deploy

### First time

````shell
sam build
sam deploy --guided --parameter-overrides Stage=staging
````

### Normal

````shell
sam build
sam deploy --config-env staging --force-upload
````

## Run test

```shell
docker run --rm --name redis-test -p 6379:6379 redis:7
cd fcm-worker
npm run unit
```

## Send message test to SQS

```shell
./test.sh
```