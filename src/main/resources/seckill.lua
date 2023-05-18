
-- 1.1--优惠券id
local voucherId = ARGV[1]

--1.2 用户id
local userId = ARGV[2]

--1.3 订单id
local orderId = ARGV[3]

-- 2.数据key
--2.1 库存key
local stockKey = 'seckill:stock:' .. voucherId

--2.2订单key
local orderKey = 'seckill:order:' .. voucherId

--脚本业务
--3.1判断库存是否充足get stockKey
if(tonumber(redis.call('get',stockKey)) <= 0) then
    --库存不足返回一
    return 1
end
--3.2 判断用户是否下单 sismember
if(redis.call('sismember',orderKey,userId) == 1)then
    --存在是重复下单
    return 2
end
--3.4 扣库存 incrby stockKey -1
redis.call('incrby',stockKey,-1)
--3.5下单
redis.call('sadd',orderKey,userId)
--3.6.发送消息到队列中， xadd stream.order *（消息id）k1 v1 k2 v2.....
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0