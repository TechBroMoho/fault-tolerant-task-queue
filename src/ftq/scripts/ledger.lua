#!lua
-- ledger.lua: apply a side effect at most once per effect key.
--
-- Emulates a downstream API that honors idempotency keys (like Stripe's): the first
-- request with a key performs the effect; repeats are acknowledged but do nothing.
-- The `#!lua` shebang (no flags) makes Redis check for OOM before the script runs, so
-- the marker and the log entry are written together or not at all.
--
-- KEYS[1]   ledger marker key for this effect key
-- KEYS[2]   effects log (append-only stream)
-- KEYS[3]   stats hash
-- ARGV[1]   effect key
-- ARGV[2]   marker TTL in seconds; 0 = never expire (ADR-010)
-- ARGV[3..] extra fields to record with the effect, as alternating name, value
--
-- Returns 1 = APPLIED (first time for this key), 0 = SUPPRESSED (already applied).
-- Re-send after a lost reply returns 0 and changes nothing but a counter: idempotent.

-- 1. Claim the key. SET NX succeeds only if no earlier call applied this effect.
local claimed
if tonumber(ARGV[2]) > 0 then
  claimed = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[2])
else
  claimed = redis.call('SET', KEYS[1], '1', 'NX')                  -- no TTL (chaos/tests)
end

if not claimed then
  -- 2a. Already applied: the "downstream" does nothing.
  redis.call('HINCRBY', KEYS[3], 'effects_suppressed', 1)
  return 0
end

-- 2b. First time: "perform" the effect by appending it to the effects log. The marker
--     alone could never reveal a duplicate (SET NX keeps one copy by definition); the
--     append-only log can, which is why the chaos verifier counts it (invariant I2).
local fields = {'key', ARGV[1]}
for i = 3, #ARGV do
  fields[#fields + 1] = ARGV[i]
end
redis.call('XADD', KEYS[2], '*', unpack(fields))
redis.call('HINCRBY', KEYS[3], 'effects_applied', 1)
return 1
