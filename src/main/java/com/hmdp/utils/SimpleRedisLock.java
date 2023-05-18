package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{
    //锁的名称
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PROFIX = "lock:";
    private static final String ID_PROFIX = UUID.randomUUID() + "-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static{
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }
    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutsec) {
        //获取线程标识
        String threadId = ID_PROFIX + Thread.currentThread().getId();
        //获取锁
        Boolean success = stringRedisTemplate.opsForValue().
                setIfAbsent(KEY_PROFIX + name, threadId, timeoutsec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
     //调用lua脚本
    stringRedisTemplate.execute(UNLOCK_SCRIPT,
            Collections.singletonList(KEY_PROFIX +name),
            ID_PROFIX + Thread.currentThread().getId());
    }

//    @Override
//    public void unlock() {
//        //获取线程标识
//        String threadId = ID_PROFIX + Thread.currentThread().getId();
//        //获取锁中的标识
//        String key = stringRedisTemplate.opsForValue().get(KEY_PROFIX + name);
//        if(threadId.equals(key)) {
//            //释放锁
//            stringRedisTemplate.delete(KEY_PROFIX + name);
//        }
//    }
}
