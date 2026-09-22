#!lua
-- requeue.lua: put one DEAD job back on the stream for a fresh set of attempts
-- (`ftq dlq requeue`).
--
-- KEYS[1]  dead-letter stream
-- KEYS[2]  stream
-- KEYS[3]  done key for this job_id (terminal-state hash)
-- KEYS[4]  stats hash
-- ARGV[1]  job_id (the DLQ entry's dlq_job_id)
--
-- Returns 1 if requeued, 0 if the job isn't DEAD (never died, already requeued, or
-- succeeded late).
--
-- The job keeps its job_id, so the effect ledger still knows which of its effects
-- already happened, and a requeue can't repeat them (ADR-019, ADR-027).
-- Re-send after a lost reply: the first run cleared the DEAD state, so a re-send
-- returns 0. There's never a second stream entry.

-- 1. Only a DEAD job can be requeued. Its record says where its DLQ entry is.
if redis.call('HGET', KEYS[3], 'state') ~= 'DEAD' then
  return 0
end
local dead_id = redis.call('HGET', KEYS[3], 'dead_entry_id')
local entry = redis.call('XRANGE', KEYS[1], dead_id, dead_id)
if #entry == 0 then
  return 0                                    -- the DLQ entry is gone; nothing to rebuild
end

-- 2. Rebuild the job's fields: drop the dlq_* metadata, and reset the attempt number to 0
--    so the job gets max_attempts fresh tries.
local src = entry[1][2]                                            -- {name, value, ...}
local fields = {}
for i = 1, #src, 2 do
  local name, value = src[i], src[i + 1]
  if string.sub(name, 1, 4) ~= 'dlq_' then
    if name == 'attempt' then
      value = '0'
    end
    fields[#fields + 1] = name
    fields[#fields + 1] = value
  end
end
if #fields == 0 then
  -- A DLQ entry made from an entry that had vanished carries metadata only. Fail
  -- before any write, so nothing half-happens.
  return redis.error_reply('NOTHING_TO_REQUEUE ' .. ARGV[1])
end

-- 3. Back on the stream, out of the DLQ, and the DEAD record cleared, so the job is
--    non-terminal again (retry.lua and dead.lua would otherwise treat it as finished).
redis.call('XADD', KEYS[2], '*', unpack(fields))
redis.call('XDEL', KEYS[1], dead_id)
redis.call('DEL', KEYS[3])

redis.call('HINCRBY', KEYS[4], 'requeued', 1)
return 1
