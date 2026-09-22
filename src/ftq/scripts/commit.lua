#!lua
-- commit.lua: the critical section. Record a job's success exactly once and remove its
-- stream entry, no matter how many times the job was delivered (SPEC §4).
--
-- The `#!lua` shebang (no flags) makes Redis check for OOM *before* the script runs, so
-- a full Redis rejects the whole commit instead of leaving it half-applied.
--
-- KEYS[1]  stream
-- KEYS[2]  done key for this job_id (hash: state, result, finished_at_ms, worker_id)
-- KEYS[3]  results log (append-only stream)
-- KEYS[4]  stats hash
-- ARGV[1]  consumer group
-- ARGV[2]  stream entry id being completed
-- ARGV[3]  job_id
-- ARGV[4]  result, JSON-encoded
-- ARGV[5]  done-key TTL in seconds; 0 = never expire (ADR-010)
-- ARGV[6]  worker id
-- ARGV[7]  enqueued_at_ms (copied into the results log for latency math)
--
-- Returns 1 = COMMITTED (first success for this job_id), 0 = DUPLICATE (suppressed).
--
-- First-wins and idempotent: ANY holder of the job may commit, with no ownership check.
-- Whoever arrives first records the result; every later commit for the same job_id
-- (a redelivered copy, a duplicate entry from a re-sent XADD, or this same commit
-- re-sent after a lost reply) only acks its entry and bumps a counter.

-- 1. Already succeeded? Then this delivery's work is a duplicate: drop it.
--    (Checks state == SUCCEEDED rather than key existence so that, from Phase 2, a
--    late success can still replace DEAD per ADR-009.)
if redis.call('HGET', KEYS[2], 'state') == 'SUCCEEDED' then
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])                    -- leave the PEL
  redis.call('XDEL', KEYS[1], ARGV[2])                             -- leave the stream (ADR-016)
  redis.call('HINCRBY', KEYS[4], 'duplicates_suppressed', 1)
  return 0
end

-- 2. First success. Timestamp from Redis's clock (ADR-020).
local t = redis.call('TIME')
local now_ms = string.format('%d', t[1] * 1000 + math.floor(t[2] / 1000))

-- 3. Record the terminal state and result. This key is what step 1 checks next time.
redis.call('HSET', KEYS[2],
  'state', 'SUCCEEDED',
  'result', ARGV[4],
  'finished_at_ms', now_ms,
  'worker_id', ARGV[6])
if tonumber(ARGV[5]) > 0 then
  redis.call('EXPIRE', KEYS[2], ARGV[5])                           -- 0 means keep forever
end

-- 4. Append to the results log. Unlike the done key, an append-only log CAN show a
--    duplicate (two entries for one job_id), so it is the chaos verifier's evidence
--    for "0 duplicate results" (invariant I2b).
redis.call('XADD', KEYS[3], '*',
  'job_id', ARGV[3],
  'worker_id', ARGV[6],
  'enqueued_at_ms', ARGV[7],
  'finished_at_ms', now_ms)

-- 5. Remove the entry: ack (out of the PEL) then delete (out of the stream), so XLEN
--    stays "undelivered + in flight". Never XTRIM MAXLEN, which can drop unacked
--    entries (ADR-016).
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])

redis.call('HINCRBY', KEYS[4], 'processed', 1)
return 1
