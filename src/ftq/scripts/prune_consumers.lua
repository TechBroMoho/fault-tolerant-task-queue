#!lua
-- prune_consumers.lua: delete idle consumer records from the group, but ONLY those that
-- own zero pending entries.
--
-- Every worker process has a unique consumer name, so each restart leaves an idle
-- consumer record behind, and they pile up (ADR-022). Deleting them is housekeeping, and
-- it's dangerous: XGROUP DELCONSUMER DISCARDS the consumer's pending entries. Those jobs
-- vanish from the PEL, no reaper can ever reclaim them, and they're silently lost. So a
-- consumer that still owns even one entry is never deleted. The reaper first moves its
-- entries to a live worker, and a later pass then finds the consumer empty (ADR-029).
--
-- KEYS[1]  stream
-- KEYS[2]  stats hash
-- ARGV[1]  consumer group
-- ARGV[2]  min idle time in ms before a consumer may be deleted
-- ARGV[3]  caller's own consumer name (never deleted)
--
-- Returns the list of deleted consumer names.
--
-- Atomicity: the check and the delete happen in one script, so no XREADGROUP or
-- XAUTOCLAIM can hand the consumer an entry between "it owns nothing" and the delete.
-- Re-send after a lost reply: the deleted consumers are gone and the rest are re-checked,
-- so a re-send is safe.

local deleted = {}

-- 1. Every consumer in the group, each as a flat {name, <n>, pending, <n>, idle, <ms>, ...}.
local consumers = redis.call('XINFO', 'CONSUMERS', KEYS[1], ARGV[1])
for _, c in ipairs(consumers) do
  local info = {}
  for i = 1, #c, 2 do
    info[c[i]] = c[i + 1]
  end
  -- 2. Candidates: not us, and idle (ms since the last read/claim attempt) past the
  --    threshold. The threshold only avoids churning live consumers; it is not what
  --    makes deletion safe.
  if info['name'] ~= ARGV[3] and info['idle'] >= tonumber(ARGV[2]) then
    -- 3. The check that makes deletion safe: ask the PEL itself whether this consumer
    --    owns any entry. XPENDING filtered by consumer, COUNT 1: {} means "owns nothing".
    local owned = redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, info['name'])
    if #owned == 0 then
      -- 4. Nothing pending, so DELCONSUMER can't drop a job. It returns the number of
      --    pending entries it discarded, which is 0 here by step 3.
      redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], info['name'])
      deleted[#deleted + 1] = info['name']
    end
  end
end

if #deleted > 0 then
  redis.call('HINCRBY', KEYS[2], 'consumers_pruned', #deleted)
end
return deleted
