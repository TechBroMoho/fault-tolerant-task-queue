#!lua
-- dead.lua: move a job to the dead-letter queue (DLQ) and record its terminal state DEAD.
-- Only the entry's current owner may do this.
--
-- Used by every path to the DLQ (ADR-027):
--   max_attempts    the handler kept raising (poison job)
--   max_deliveries  the entry kept being redelivered: the job keeps crashing its worker
--   malformed       the entry can't be parsed as a job
--   unknown_type    no handler is registered for the job's type
--
-- KEYS[1]  stream
-- KEYS[2]  dead-letter stream
-- KEYS[3]  done key for this job_id (terminal-state hash)
-- KEYS[4]  stats hash
-- ARGV[1]  consumer group
-- ARGV[2]  stream entry id
-- ARGV[3]  caller's consumer name (the worker id)
-- ARGV[4]  job_id (for a malformed entry without one: "entry:<entry id>")
-- ARGV[5]  reason (one of the four above)
-- ARGV[6]  last error message
-- ARGV[7]  attempts made (handler runs started; 0 if the entry never parsed)
-- ARGV[8]  '1' if the last run timed out (ADR-030), else '0'
--
-- Returns 'OK' (moved to the DLQ), 'LEASE_LOST' (not the owner: nothing changed), or
-- 'TERMINAL' (another copy of this job already finished: this entry is dropped).
--
-- Precedence (SPEC §4, ADR-009): DEAD never replaces SUCCEEDED, which step 2 enforces. A
-- late SUCCESS may replace DEAD; that branch is in commit.lua.
-- Re-send after a lost reply: the first run acked the entry, so the re-send returns
-- LEASE_LOST and adds no second DLQ entry (ADR-006).

-- 1. Ownership: XPENDING for one id returns {{id, owner, idle_ms, deliveries}} or {}.
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 or p[1][2] ~= ARGV[3] then
  redis.call('HINCRBY', KEYS[4], 'lease_lost', 1)
  return 'LEASE_LOST'                                              -- not ours: touch nothing
end
local deliveries = p[1][4]

-- 2. Terminal state already set by another copy of this job_id? SUCCEEDED must never be
--    overwritten by DEAD, and a second DLQ entry for one job would be a duplicate. Drop
--    this copy instead.
if redis.call('HGET', KEYS[3], 'state') then
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
  redis.call('XDEL', KEYS[1], ARGV[2])
  redis.call('HINCRBY', KEYS[4], 'duplicates_suppressed', 1)
  return 'TERMINAL'
end

-- 3. Build the DLQ entry: the job's original fields verbatim (so `dlq requeue` can
--    rebuild the job), plus dlq_* metadata saying why it died.
local entry = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2])
local fields = {}
if #entry > 0 then
  fields = entry[1][2]                                             -- {name, value, ...}
end
local t = redis.call('TIME')                                       -- Redis clock (ADR-020)
local now_ms = string.format('%d', t[1] * 1000 + math.floor(t[2] / 1000))
local meta = {
  'dlq_job_id', ARGV[4],
  'dlq_reason', ARGV[5],
  'dlq_error', ARGV[6],
  'dlq_attempts', ARGV[7],
  'dlq_deliveries', tostring(deliveries),
  'dlq_dead_at_ms', now_ms,
  'dlq_worker_id', ARGV[3],
  'dlq_source_entry_id', ARGV[2],
}
for i = 1, #meta do
  fields[#fields + 1] = meta[i]
end
local dead_id = redis.call('XADD', KEYS[2], '*', unpack(fields))

-- 4. Terminal state DEAD. dead_entry_id lets a late success (commit.lua) and
--    `dlq requeue` find this job's DLQ entry. No TTL: a DEAD record lives as long as its
--    DLQ entry, which only a requeue or a late success removes.
redis.call('HSET', KEYS[3],
  'state', 'DEAD',
  'dead_entry_id', dead_id,
  'reason', ARGV[5],
  'error', ARGV[6],
  'finished_at_ms', now_ms,
  'worker_id', ARGV[3])

-- 5. Remove the entry from the PEL and the stream (ADR-016).
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])

redis.call('HINCRBY', KEYS[4], 'dead', 1)
if ARGV[8] == '1' then
  redis.call('HINCRBY', KEYS[4], 'timeouts', 1)                    -- counted with the move
end
return 'OK'
