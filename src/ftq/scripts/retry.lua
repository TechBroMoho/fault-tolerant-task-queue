#!lua
-- retry.lua: after a handler failure, move the job from the stream to the delayed set,
-- to run again after a backoff. Only the entry's current owner may do this.
--
-- KEYS[1]  stream
-- KEYS[2]  delayed set (zset: member = JSON-encoded job fields, score = due time in ms)
-- KEYS[3]  done key for this job_id (terminal-state hash)
-- KEYS[4]  stats hash
-- ARGV[1]  consumer group
-- ARGV[2]  stream entry id that failed
-- ARGV[3]  caller's consumer name (the worker id)
-- ARGV[4]  next attempt number (the failed attempt + 1)
-- ARGV[5]  delay in ms (full-jitter backoff, computed by the caller: backoff.py)
-- ARGV[6]  '1' if the failed run timed out (ADR-030), else '0'
--
-- Returns 'OK' (retry scheduled), 'LEASE_LOST' (the caller isn't the owner: nothing
-- changed), or 'TERMINAL' (the job already succeeded or died via another copy: this
-- entry is dropped, nothing is scheduled).
--
-- Why the ownership check: picture worker A stalling past its lease; worker B reclaims
-- the job and commits it. When A wakes up and its handler raises, A must NOT schedule a
-- retry, or the finished job would run again. A no longer owns the entry (B acked it), so
-- this returns LEASE_LOST (SPEC §4, ADR-024).
-- Re-send after a lost reply: the first run already acked the entry, so the re-send
-- returns LEASE_LOST and changes nothing (ADR-006).

-- 1. Ownership. A one-id XPENDING range returns {{id, owner, idle_ms, deliveries}}, or
--    {} if the entry is no longer pending.
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 or p[1][2] ~= ARGV[3] then
  redis.call('HINCRBY', KEYS[4], 'lease_lost', 1)
  return 'LEASE_LOST'                                              -- not ours: touch nothing
end

-- 2. Terminal state. We own this entry, but another copy of the same job_id (a duplicate
--    entry from a re-sent XADD) may already have finished. Then retrying would re-run a
--    finished job, so drop this copy instead: ack + delete, count it as a duplicate.
if redis.call('HGET', KEYS[3], 'state') then
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
  redis.call('XDEL', KEYS[1], ARGV[2])
  redis.call('HINCRBY', KEYS[4], 'duplicates_suppressed', 1)
  return 'TERMINAL'
end

-- 3. Copy the job's fields from the entry itself, so the retry is exactly the job that
--    was enqueued (same job_id, payload, idempotency key, enqueued_at_ms).
local entry = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2])
if #entry == 0 then
  -- Pending but deleted can't happen here (every exit acks before it deletes). Fail
  -- before any write, so the entry stays in the PEL for the reaper.
  return redis.error_reply('ENTRY_MISSING ' .. ARGV[2])
end
local fields = entry[1][2]                                         -- {name, value, ...}
for i = 1, #fields, 2 do
  if fields[i] == 'attempt' then
    fields[i + 1] = ARGV[4]                                        -- bump the attempt number
  end
end

-- 4. Due time on Redis's clock (ADR-020), so every worker's scheduler agrees on it.
local t = redis.call('TIME')                                       -- {seconds, microseconds}
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local due = string.format('%d', now_ms + tonumber(ARGV[5]))

-- 5. Park the job in the delayed set. schedule.lua moves it back into the stream once due.
redis.call('ZADD', KEYS[2], due, cjson.encode(fields))

-- 6. Remove the failed entry: out of the PEL and out of the stream (ADR-016).
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])

redis.call('HINCRBY', KEYS[4], 'retried', 1)
if ARGV[6] == '1' then
  redis.call('HINCRBY', KEYS[4], 'timeouts', 1)                    -- counted with the retry
end
return 'OK'
