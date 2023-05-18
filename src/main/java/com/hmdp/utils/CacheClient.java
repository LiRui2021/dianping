package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringredistemplate) {
        this.stringRedisTemplate = stringredistemplate;
    }
    public void set(String key, Object value, Long time, TimeUnit unit){
       stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time ,unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisDate = new RedisData();
        redisDate.setData(value);
        redisDate.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisDate));
    }
    public <R,ID> R queryWithPassThrough(String keyPrefix,ID id, Class<R> type,Function<ID,R> dbFallback,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否存在
        //存在，直接返回
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        if(json != null){
            return null;
        }
        //不存在
        //1.查数据库
        R r = dbFallback.apply(id);
        //2.判断商铺是否存在
        //不存在，返回错误
        if(r == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //3.如果存在，写入Redis，返回数据
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r),time, unit);
        return r;

    }


    public <R,ID> R queryWithLogicExpire(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //从Redis查询商铺缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否存在
        ////不存在，直接返回空
        if(StrUtil.isBlank(Json)) {
            return null;
        }
        //命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject,type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        //未过期，返回店铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //过期，需要缓存重建
        //获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean tryLock = tryLock(lockKey);
        //判断时否获取成功
        //成功，开启独立线程，实现缓存重建
        if(tryLock){
            new Thread(){
                @Override
                public void run() {
                    try {
                        R r1 = dbFallback.apply(id);
                        setWithLogicalExpire(key,r1,time,unit);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    } finally {
                        unlock(lockKey);
                    }
                }
            }.start();
        }

        //返回过期商铺信息

        return r;

    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }


}
