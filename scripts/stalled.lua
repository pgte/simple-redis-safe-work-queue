local queueName = KEYS[1]

local timeoutQueue = ARGV[1]
local pendingQueue = ARGV[2]
local stalledQueue = ARGV[3]
local now = ARGV[4]
local item = ARGV[5]

redis.log(redis.LOG_DEBUG, "stalled? " .. item)

local items = redis.call('lrange', stalledQueue, 0, -1);

for i, workId in ipairs(items) do
  if workId == item then
    redis.call('echo', 'unstalling ' .. workId .. ' <<<<<<<<<<<<<');
    local key = queueName .. '#' .. workId
    local timeout = tonumber(redis.call('hget', key, 'timeout')) + now

    redis.call('zrem', timeoutQueue, workId)
    redis.call('zadd', timeoutQueue, timeout, workId)
    redis.call('lrem', stalledQueue, 1, workId)
    redis.call('lrem', pendingQueue, 1, workId)
    redis.call('lpush', pendingQueue, workId)

    redis.call('echo', '>>>>>>>>>>>>>>>>>>')

    return workId

  end
end