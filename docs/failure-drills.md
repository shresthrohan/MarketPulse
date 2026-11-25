# Failure drills

Run 2026-11-25T01:32:05+00:00 with `python scripts/failure_drills.py` against the local compose stack. Each drill breaks one real component and asserts recovery.

| drill | result | seconds | evidence |
|---|---|---|---|
| redis_outage | PASS | 15.9 | API served 200 with cache.available=false and storage continued during the outage; quotes fresh again after restart |
| redis_data_loss | PASS | 16.8 | 20/20 quotes back after FLUSHALL (20 rebuilt from PostgreSQL), each equal to the latest stored event; simulator resumed within 0.002% of the last price |
| processor_kill | PASS | 48.4 | killed with SIGKILL; backlog 195 drained after restart; +1154 rows, 0 duplicate ids |
| offset_rewind_replay | PASS | 14.6 | rewound 1200 offsets; replay produced 1198 duplicate warnings and 0 new rows for already-stored events |
| postgres_restart | PASS | 3.8 | processor retried through the outage without restarting; API recovered; 15 event(s) stored as retried in total |
| backend_restart | PASS | 5.2 | backend recreated; REST healthy and a live quote received over WebSocket through nginx |
| kafka_restart | PASS | 7.1 | broker restarted; ingestion and processor reconnected by themselves and events flow again |
