local timeoutQueue = ARGV[1]
local pendingQueue = ARGV[2]
local stalledQueue = ARGV[3]
local now = tonumber(ARGV[4])

local items = redis.call('zrangebyscore', timeoutQueue, 0, now);

for i, workId in ipairs(items) do
  redis.call('lpush', pendingQueue, workId)
  redis.call('zrem', timeoutQueue, workId)
  redis.call('lrem', stalledQueue, 1, workId)
end

return items