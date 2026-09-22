#!lua
-- heartbeat.lua: extend the lease on one stream entry, but only for its current owner.
--
-- A lease is the entry's idle time in the consumer group's Pending Entries List (PEL):
-- the reaper reclaims entries idle longer than the visibility timeout. Resetting the idle
-- time is how a running job says "I'm still alive" (ADR-025).
--
-- KEYS[1]  stream
-- KEYS[2]  stats hash
-- ARGV[1]  consumer group
-- ARGV[2]  stream entry id
-- ARGV[3]  caller's consumer name (the worker id)
--
-- Returns 'OK' (lease extended) or 'LEASE_LOST' (the caller no longer owns the entry).
--
-- Why the ownership check: XCLAIM doesn't check who owns an entry. Without the check, a
-- worker whose job was reclaimed would XCLAIM it straight back, and two workers could
-- steal one lease back and forth forever (SPEC §4, ADR-024).
-- Re-send after a lost reply: harmless. The second run either resets the idle time again
-- (still the owner) or returns LEASE_LOST.
-- No terminal-state check, unlike retry/dead: a heartbeat only moves the lease clock and
-- can't overwrite an outcome, so it has nothing to protect (ADR-024).

-- 1. Who owns the entry right now? A one-id XPENDING range returns
--    {{id, owner, idle_ms, delivery_count}}, or {} if the entry is no longer pending
--    (it was committed, retried, or moved to the DLQ).
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 or p[1][2] ~= ARGV[3] then
  redis.call('HINCRBY', KEYS[2], 'lease_lost', 1)
  return 'LEASE_LOST'                                              -- not ours: touch nothing
end

-- 2. XCLAIM to ourselves with min-idle-time 0 resets the entry's idle time to 0.
--    JUSTID keeps the delivery counter unchanged (verified on Redis 8.8.3, and asserted by
--    a test), so heartbeats never push a job toward max_deliveries.
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID')
redis.call('HINCRBY', KEYS[2], 'heartbeats', 1)
return 'OK'
