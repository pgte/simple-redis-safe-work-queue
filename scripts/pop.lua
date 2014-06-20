local baseQueueName = KEYS[1]
local pendingQueue = KEYS[2]
local timeoutQueue = KEYS[3]

local time = ARGV[1]

local id = redis.lindex(pendingQueue, -1)

if id <> nil
  local work = redis.hmget(baseQueueName .. '#' .. id)

  redis.multi()
  redis.rpop(pendingQueue)
  redis.zadd(timeoutQueue, time + work.timeout)
  redis.exec()

  return work

end
