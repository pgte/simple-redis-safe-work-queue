local queueName = KEYS[1]

local timeoutQueue = ARGV[1]
local pendingQueue = ARGV[2]
local stalledQueue = ARGV[3]
local now = tonumber(ARGV[4])
local stalledTimeout = tonumber(ARGV[5])
local item = ARGV[6]

local items = redis.call('lrange', stalledQueue, 0, -1);

for i, workId in ipairs(items) do
  if workId == item then

    local key = queueName .. '#' .. workId
    local timeout = tonumber(redis.call('hget', key, 'timeout'))

    -- calculate how long this one has been waiting for
    local timesoutAt = tonumber(redis.call('zscore', timeoutQueue, workId))

    local willUnstall = true

    if timesoutAt then
      local queuedFor = timesoutAt - timeout
      willUnstall = (queuedFor >= stalledTimeout)
    end

    if not willUnstall then return end

    redis.call('zrem', timeoutQueue, workId)
    redis.call('zadd', timeoutQueue, timeout + now, workId)
    redis.call('lrem', stalledQueue, 1, workId)
    redis.call('lrem', pendingQueue, 1, workId)
    redis.call('lpush', pendingQueue, workId)

    return workId

  end
end