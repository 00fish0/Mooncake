# Concurrent OpLog Failover Reproducer

This reproducer checks two HA correctness properties under concurrent client
traffic:

1. An acknowledged `Put` must not lose its `PUT_END` OpLog record when the
   ordered writer is under admission pressure.
2. A surviving client must be able to read its replicas after reconnecting and
   remounting its segment on the replacement Primary.

The test starts three Masters with the `etcd_batch_record` backend, runs 16
threads that repeatedly perform a unique-key `Put` followed by an immediate
single `Get`, kills the current Primary, waits for a replacement Primary and
the same client to reconnect, and then continues the workload. It also compares
single-key behavior with `BatchPut`/`BatchGet` and verifies the final durable
OpLog prefix.

## Build and run

```bash
cmake --build build --target oplog_concurrent_failover_client -j2

LD_LIBRARY_PATH="$PWD/build/mooncake-common/etcd:$PWD/build/mooncake-common" \
  mooncake-store/tests/e2e/run_oplog_concurrent_failover.sh \
  --build-dir "$PWD/build"
```

The script intentionally exits with a nonzero status when it reproduces a
correctness failure. All artifacts are preserved in the printed run directory.
The main files are:

- `workload/result-summary.json`: combined pass/fail assessment
- `workload/control/summary.json`: per-phase client results and error codes
- `audit/verify.json`: final OpLog continuity and checksum verification
- `logs/master-*.err`: Master logs used to count OpLog admission failures

Useful workload options include `--threads`, `--pre-seconds`, `--post-seconds`,
`--value-bytes`, `--segment-bytes`, and `--batch-entries`.

## Result on `e372c2d1`

The packaged reproducer was run with 16 threads and two seconds in each steady
phase:

```bash
run_oplog_concurrent_failover.sh --pre-seconds 2 --post-seconds 2
```

Before the fault, all 23,858 Put/Get pairs succeeded on the active Primary,
while 2,788 acknowledged `PutEnd` operations failed OpLog admission with
`TASK_PENDING_LIMIT_EXCEEDED` (`-1401`). After Master-0 was killed, Master-1
became Primary and the same client reconnected. All 17,195 post-reconnection
Puts succeeded, but every immediate single Get returned
`REPLICA_IS_NOT_READY`. The replacement Primary reported another 1,522
`PutEnd` admission failures.

The final focused probe produced:

```text
Put             -> OK
single Get      -> REPLICA_IS_NOT_READY
BatchPut/Get    -> OK
```

Inspector verification passed for 2,429 batches and 47,441 entries with no
gaps, orphan batches, or checksum errors. That result only verifies records
that reached etcd; it cannot detect acknowledged operations that were never
admitted into the OpLog.
