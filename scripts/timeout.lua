local queueName = KEYS[1]

local timeoutQueue = ARGV[1]
local pendingQueue = ARGV[2]
local stalledQueue = ARGV[3]
local now = tonumber(ARGV[4])

local items = redis.call('zrangebyscore', timeoutQueue, 0, now);

for i, workId in ipairs(items) do
  local key = queueName .. '#' .. workId
  local timeout = tonumber(redis.call('hget', key, 'timeout')) + now

  redis.call('lrem', pendingQueue, 1, workId)
  redis.call('lpush', pendingQueue, workId)
  redis.call('zrem', timeoutQueue, workId)
  redis.call('zadd', timeoutQueue, timeout, workId)
  redis.call('lrem', stalledQueue, 1, workId)
end

return items