#!/bin/bash
# Two concurrent LoadGenerator processes against ONE server. Used to tell apart "single-client bottleneck"
# from "server bottleneck": if sum(achieved) >> single-client ceiling, the single client was the limit.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CP="$ROOT/target/classes:$ROOT/target/lib/*"
PROTO=${1:-rest}
RPS_EACH=${2:-10000}
DURATION=${3:-10}
WARMUP=${4:-5}
THREADS=${5:-256}
PORT=$((50080 + RANDOM % 100))

# Start server
java -Xms512m -Xmx512m -XX:+UseG1GC -cp "$CP" sis2.app.ServerMain --proto $PROTO --port $PORT --threads 16 > /tmp/two-srv-$PROTO.log 2>&1 &
SRV_PID=$!

# Wait for READY
for i in $(seq 1 60); do
    grep -q "READY" /tmp/two-srv-$PROTO.log 2>/dev/null && break
    sleep 0.5
done
if ! grep -q "READY" /tmp/two-srv-$PROTO.log 2>/dev/null; then
    echo "server failed to start" >&2
    cat /tmp/two-srv-$PROTO.log >&2
    kill $SRV_PID 2>/dev/null
    exit 1
fi

# Two concurrent LoadGenerators
java -Xms256m -Xmx768m -XX:+UseG1GC -cp "$CP" sis2.bench.LoadGenerator \
    --proto $PROTO --port $PORT --rps $RPS_EACH --duration $DURATION --warmup $WARMUP \
    --threads $THREADS --size 1024 > /tmp/two-lg1-$PROTO.csv 2> /tmp/two-lg1-$PROTO.err &
PID1=$!
java -Xms256m -Xmx768m -XX:+UseG1GC -cp "$CP" sis2.bench.LoadGenerator \
    --proto $PROTO --port $PORT --rps $RPS_EACH --duration $DURATION --warmup $WARMUP \
    --threads $THREADS --size 1024 > /tmp/two-lg2-$PROTO.csv 2> /tmp/two-lg2-$PROTO.err &
PID2=$!

wait $PID1
RC1=$?
wait $PID2
RC2=$?

kill $SRV_PID 2>/dev/null
wait $SRV_PID 2>/dev/null

echo "=== server (last 10 lines) ==="
tail -10 /tmp/two-srv-$PROTO.log
echo ""
echo "=== client 1 (rc=$RC1) ==="
cat /tmp/two-lg1-$PROTO.csv
echo "=== client 2 (rc=$RC2) ==="
cat /tmp/two-lg2-$PROTO.csv
