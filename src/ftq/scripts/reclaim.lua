#!lua
-- reclaim.lua: the reaper. Take over entries whose lease expired (their owner crashed,
-- stalled, or was partitioned) and report each one's delivery count.
--
-- KEYS[1]  stream
-- KEYS[2]  stats hash
-- ARGV[1]  consumer group
-- ARGV[2]  caller's consumer name (the worker id): the new owner
-- ARGV[3]  min idle time in ms (= the visibility timeout, the lease length)
-- ARGV[4]  scan cursor (an entry id; '0-0' starts from the beginning of the PEL)
-- ARGV[5]  max entries to claim (the caller's free slots, so the in-flight cap holds)
--
-- Returns {next_cursor, {{entry_id, {field, value, ...}, delivery_count}, ...}, deleted_ids}.
--
-- Why a script instead of a bare XAUTOCLAIM: the worker needs each entry's delivery
-- count to spot crash-looping jobs (count > max_deliveries goes to the DLQ), and
-- XAUTOCLAIM doesn't return it. Reading it here costs no extra round trip, and the
-- `reclaimed` counter is bumped in the same atomic step as the claim.
-- Re-send after a lost reply: the entries were already claimed (idle reset to 0), so a
-- re-send finds nothing new to claim. The claimed entries sit in our PEL unworked, which
-- looks exactly like a crashed worker, so a later pass reclaims them again (ADR-006).

-- 1. Claim up to ARGV[5] entries idle for at least ARGV[3] ms. XAUTOCLAIM transfers
--    ownership, resets idle to 0, and increments each entry's delivery counter.
local r = redis.call('XAUTOCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4],
  'COUNT', ARGV[5])
local cursor, claimed, deleted = r[1], r[2], r[3]

-- 2. Attach each claimed entry's (just incremented) delivery count.
local out = {}
for _, e in ipairs(claimed) do
  local p = redis.call('XPENDING', KEYS[1], ARGV[1], e[1], e[1], 1)
  out[#out + 1] = {e[1], e[2], p[1][4]}
end

if #out > 0 then
  redis.call('HINCRBY', KEYS[2], 'reclaimed', #out)
end

-- deleted: ids that were pending but whose stream entry no longer exists. XAUTOCLAIM
-- has already dropped them from the PEL. Our own exits always ack before they delete,
-- so this should stay empty; the worker logs any it sees.
return {cursor, out, deleted}
