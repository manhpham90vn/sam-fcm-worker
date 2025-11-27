#!/usr/bin/env bash
set -euo pipefail

QUEUE_URL="https://sqs.ap-southeast-1.amazonaws.com/047590809543/fcm-main-queue-sqs-staging"
AWS_REGION="ap-southeast-1"

for i in $(seq 1 100); do
  if (( RANDOM % 2 )); then
    BODY=$(jq -nc \
      --arg type "topic" \
      --arg title "test-topic-$i" \
      --arg body "body-topic-$RANDOM" \
      --arg topic "all" \
      '{type:$type, title:$title, body:$body, topic:$topic}')
  else
    BODY=$(jq -nc \
      --arg type "tokens" \
      --arg title "test-tokens-$i" \
      --arg body "body-tokens-$RANDOM" \
      --argjson tokens '["abc","def","ghi","xyz"]' \
      '{type:$type, title:$title, body:$body, tokens:$tokens}')
  fi

  echo "Sending message $i: $BODY"

  aws sqs send-message \
    --region "$AWS_REGION" \
    --queue-url "$QUEUE_URL" \
    --message-body "$BODY" \
    --no-cli-pager \
    > /dev/null
done
