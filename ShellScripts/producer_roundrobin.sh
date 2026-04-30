#!/usr/bin/env bash
set -euo pipefail

# 環境変数チェック
: "${KAFKA_PATH:?環境変数 KAFKA_PATH を設定してください。}"
: "${BOOTSTRAP_SERVER:?環境変数 BOOTSTRAP_SERVER を設定してください。}"

# 引数設定
TOPIC="${1:?1つ目の引数にTopic名を指定してください}"
INTERVAL_SEC="${2:-10}"   # デフォルト10(s)

# 変数設定
PRODUCER_CONFIG="${KAFKA_PATH}/bin/client.properties"
PRODUCER_BIN="${KAFKA_PATH}/bin/kafka-console-producer.sh"

# store_idの候補
STORE_IDS=("001" "002" "003" "004" "005")

# amountの範囲
AMOUNT_MIN=100
AMOUNT_MAX=5000

# 終了処理
cleanup() {
  echo "Stopping..." >&2
}
trap cleanup INT TERM

# JSONイベントを継続生成してstdoutへ出す（producerにパイプする）
generate() {
  while true; do
    store_id="${STORE_IDS[$((RANDOM % ${#STORE_IDS[@]}))]}"

    # amount: [AMOUNT_MIN, AMOUNT_MAX]
    amount=$(( AMOUNT_MIN + (RANDOM % (AMOUNT_MAX - AMOUNT_MIN + 1)) ))

    # timestamp: 例 2026-03-07 10:00:00
    ts="$(date '+%Y-%m-%d %H:%M:%S')"

    # 1行JSON
    printf '{"store_id":"%s","amount":%d,"timestamp":"%s"}\n' "$store_id" "$amount" "$ts"

    # 指定間隔待つ
    sleep "$INTERVAL_SEC"
  done
}

echo "Producing to topic=${TOPIC}, interval=${INTERVAL_SEC}s"
echo "Using bootstrap=${BOOTSTRAP_SERVER}"
echo "Using config=${PRODUCER_CONFIG}"

# producerの実行
generate | "${PRODUCER_BIN}" \
  --broker-list "${BOOTSTRAP_SERVER}" \
  --producer.config "${PRODUCER_CONFIG}" \
  --topic "${TOPIC}"