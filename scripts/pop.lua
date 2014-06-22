local baseQueueName = KEYS[1]
local pendingQueue = KEYS[2]
local timeoutQueue = KEYS[3]
local stalledQueue = KEYS[4]

local time = tonumber(ARGV[1])
local popTimeout = tonumber(ARGV[2])

local id = redis.call('brpoplpush', pendingQueue, stalledQueue, popTimeout)


if id then
  local hgetall = function (key)
    local bulk = redis.call('HGETALL', key)
    local result = {}
    local nextkey

    for i, v in ipairs(bulk) do
      if i % 2 == 1 then
        nextkey = v
      else
        result[nextkey] = v
      end
    end

    return { bulk = bulk, result = result }
  end

  local work = hgetall(baseQueueName .. '#' .. id)

  redis.call('zadd', timeoutQueue, time + tonumber(work.result.timeout), id)

  return work.bulk

end
