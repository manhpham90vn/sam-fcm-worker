QUEUE_URL=https://sqs.ap-southeast-1.amazonaws.com/047590809543/fcm-main-queue
BODY_TOPIC='
{
  "type": "topic",
  "title": "test",
  "body": "test",
  "topic": "all"
}
'

BODY_TOKENS='
{
  "type": "tokens",
  "title": "test",
  "body": "test",
  "tokens": ["abc", "def"]
}
'

aws sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body "$BODY_TOPIC"
