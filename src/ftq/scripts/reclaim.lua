#!lua
-- reclaim.lua: the reaper. Take over entries whose lease expired (their owner crashed,
-- stalled, or was partitioned) and report each one's delivery count.
--
-- KEYS[1]  stream
-- KEYS[2]  stats hash
-- KEYS[3]  reclaim histogram (hash: delivery count after the claim -> entries claimed)
-- ARGV[1]  consumer group
-- ARGV[2]  caller's consumer name (the worker id): the new owner
-- ARGV[3]  min idle time in ms (= the visibility timeout, the lease length)
-- ARGV[4]  scan cursor (an entry id; '0-0' starts from the beginning of the PEL)
-- ARGV[5]  max entries to claim (the caller's free slots, so the in-flight cap holds)
-- ARGV[6]  suspect slots: how many suspects the caller may take now (0 or 1)
-- ARGV[7]  suspect threshold (Settings.suspect_threshold, never above max_deliveries): an
--          entry whose delivery count (after this claim) is at least this, and at most
--          ARGV[8], is a suspect (ADR-035)
-- ARGV[8]  max_deliveries (an entry past it goes to the DLQ unrun: never a suspect)
--
-- Returns {next_cursor, {{entry_id, {field, value, ...}, delivery_count}, ...}, deleted_ids}.
--
-- Why a script instead of a bare XAUTOCLAIM: the worker needs each entry's delivery
-- count to spot crash-looping jobs (count > max_deliveries goes to the DLQ), and
-- XAUTOCLAIM doesn't return it. Reading it here costs no extra round trip, and the
-- `reclaimed` counter is bumped in the same atomic step as the claim.
--
-- Why suspects: a job that crashes its worker takes down every job running beside it.
-- Their entries expire together, and if one reaper claimed them all again, they would
-- crash the next worker together, pick up a delivery each time, and reach max_deliveries
-- with the crashy job: dead-lettered for nothing. So an entry that has already been
-- redelivered (count >= threshold) is a suspect, and each worker runs at most one
-- suspect at a time. Companions then go to different workers (ADR-035).
--
-- Re-send after a lost reply: the entries were already claimed (idle reset to 0), so a
-- re-send finds nothing new to claim. The claimed entries sit in our PEL unworked, which
-- looks exactly like a crashed worker, so a later pass reclaims them again (ADR-006).

-- 1. Claim up to ARGV[5] entries idle for at least ARGV[3] ms. XAUTOCLAIM transfers
--    ownership, resets idle to 0, and increments each entry's delivery counter.
local r = redis.call('XAUTOCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4],
  'COUNT', ARGV[5])
local cursor, claimed, deleted = r[1], r[2], r[3]

local suspect_slots = tonumber(ARGV[6])
local threshold = tonumber(ARGV[7])
local max_deliveries = tonumber(ARGV[8])

-- 2. Attach each claimed entry's (just incremented) delivery count, and keep only the
--    suspects the caller has room for.
local out = {}
for _, e in ipairs(claimed) do
  local p = redis.call('XPENDING', KEYS[1], ARGV[1], e[1], e[1], 1)
  local deliveries = p[1][4]
  local suspect = deliveries >= threshold and deliveries <= max_deliveries
  if suspect and suspect_slots == 0 then
    -- 2a. No room for another suspect: put the entry back exactly as it was, as if this
    --     pass had never claimed it. Delivery count restored (RETRYCOUNT, and JUSTID so
    --     this XCLAIM doesn't bump it), idle set back to the lease so it is still
    --     expired and any worker with a free suspect slot takes it at once. Nobody can
    --     see the round trip: the script is atomic.
    redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, e[1],
      'IDLE', ARGV[3], 'RETRYCOUNT', deliveries - 1, 'JUSTID')
  else
    -- 2b. Keep it. Record how far its delivery count has climbed.
    if suspect then
      suspect_slots = suspect_slots - 1
    end
    out[#out + 1] = {e[1], e[2], deliveries}
    redis.call('HINCRBY', KEYS[3], tostring(deliveries), 1)
  end
end

if #out > 0 then
  redis.call('HINCRBY', KEYS[2], 'reclaimed', #out)
end

-- deleted: ids that were pending but whose stream entry no longer exists. XAUTOCLAIM
-- has already dropped them from the PEL. Our own exits always ack before they delete,
-- so this should stay empty; the worker logs any it sees.
return {cursor, out, deleted}
