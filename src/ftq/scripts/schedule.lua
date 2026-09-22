#!lua
-- schedule.lua: move retries whose backoff has elapsed from the delayed set back into
-- the stream, where any worker can pick them up.
--
-- KEYS[1]  delayed set (member = JSON-encoded job fields, score = due time in ms)
-- KEYS[2]  stream
-- KEYS[3]  stats hash
-- ARGV[1]  max jobs to move in this call
--
-- Returns the number of jobs moved.
--
-- Every worker runs this loop concurrently. That's safe because each call is atomic:
-- a due member is removed and re-added to the stream in one step, so two schedulers can
-- never both move the same retry (ADR-026).
-- Re-send after a lost reply: the moved members are gone from the set, so a re-send
-- moves only jobs that became due since. Nothing moves twice.

-- 1. "Now" on Redis's clock: the same clock retry.lua used to compute the due time.
local t = redis.call('TIME')
local now_ms = string.format('%d', t[1] * 1000 + math.floor(t[2] / 1000))

-- 2. The oldest due members, at most ARGV[1] of them.
local due = redis.call('ZRANGE', KEYS[1], '-inf', now_ms, 'BYSCORE', 'LIMIT', 0, ARGV[1])

-- 3. Move each one: out of the delayed set, into the stream as a fresh entry (delivery
--    count starts at 1 again; the attempt number was already bumped by retry.lua).
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('XADD', KEYS[2], '*', unpack(cjson.decode(member)))
end

if #due > 0 then
  redis.call('HINCRBY', KEYS[3], 'scheduled', #due)
end
return #due
