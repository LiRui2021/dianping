package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@SuppressWarnings("all")
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisWorker redisWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private String queueName = "stream.orders";

    private  IVoucherOrderService proxy;

    //初始化lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //阻塞队列
    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    //线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//    @PostConstruct
//    private void init(){
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//    private class  VoucherOrderHandler implements  Runnable{
//        @Override
//        public void run() {
//         while(true){
//             //1.获取队列中的订单信息
//             try {
//                 VoucherOrder voucherOrder = orderTasks.take();
//                 //2.创建订单
//               handlerVoucherOrder(voucherOrder);
//             } catch (Exception e) {
//                 log.error("处理订单信息异常",e);
//             }
//
//         }
//        }
//    }
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class  VoucherOrderHandler implements  Runnable{
        @Override
        public void run() {
         while(true){
             //1.获取消息队列中的订单信息 xREADGROUP GROUP g1 c1 count 1 BLOCK 2000 STREAMS streams.order >
             try {
                 //2.判断消息是否获取成功
                 List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                         Consumer.from("g1", "c1"),
                         StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                         StreamOffset.create(queueName, ReadOffset.lastConsumed())
                 );
                 //2.1如果获取失败，说明没有消息，继续下一次循环
                if(list==null || list.isEmpty()){
                    continue;
                }
                //3.解析消息
                 MapRecord<String, Object, Object> record = list.get(0);
                 Map<Object, Object> map = record.getValue();
                 VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                 //2.若果有消息，可以下单
                 handlerVoucherOrder(voucherOrder);
                 //4.ack确认 SACK stream.orders g1 id
                   stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
             } catch (Exception e) {
                 log.error("处理订单信息异常",e);
                 handlerPendingList();
             }

         }

        }
        private void handlerPendingList() {
            while(true){
                //1.获取消息队列中的订单信息 xREADGROUP GROUP g1 c1 count 1 BLOCK 2000 STREAMS streams.order >
                try {
                    //2.判断消息是否获取成功
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.1如果获取失败，说明pending-list没有消息，结束循环
                    if(list==null || list.isEmpty()){
                        break;
                    }
                    //3.解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                    //2.若果有消息，可以下单
                    handlerVoucherOrder(voucherOrder);
                    //4.ack确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单信息异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }

            }
        }

    }

    public void handlerVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if(!isLock){
            //获取锁失败，返回错误或重试
            log.error("不允许重复下单");
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        long orderId = redisWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), UserHolder.getUser().getId().toString()
                ,String.valueOf(orderId)
        );
        // 2.判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            //2.1补位0，代表没购买资格
            return Result.fail(r==1 ? "库存不足" : "不能重复下单");
        }
        //3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), UserHolder.getUser().getId().toString()
//        );
//        // 2.判断结果是否为0
//          int r = result.intValue();
//           if(r != 0){
//               //2.1补位0，代表没购买资格
//               return Result.fail(r==1 ? "库存不足" : "不能重复下单");
//           }
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(UserHolder.getUser().getId());
//        voucherOrder.setVoucherId(voucherId);
//        //2.2为0，有购买资格,吧下单信息保存到阻塞队列
//         orderTasks.add(voucherOrder);
//
//         //3.获取代理对象
//         proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //3.返回订单id
//        return Result.ok(orderId);
//    }



//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//          return Result.fail("秒杀尚未开始");
//        }
//        //3.判断秒杀是否结束
//          if(voucher.getEndTime().isBefore(LocalDateTime.now())) {
//          return Result.fail("秒杀已经结束");
//          }
//        //4.判断库存是否充足
//         if(voucher.getStock() < 1){
//             return Result.fail("库存不足");
//         }
//
//        Long userId = UserHolder.getUser().getId();
//       //
//        // SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if(!isLock){
//            //获取锁失败，返回错误或重试
//            return Result.fail("一个人只允许下一单");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//
//    }
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //6.一人一单
        Long userId = voucherOrder.getUserId();
            //6.1查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();

            //6.2判断是否存在
            if (count > 0) {
                //用户已经购买过
               log.error("用户已购买过");
            }
        //5.扣减库存
        boolean success = seckillVoucherService.update().
                setSql("stock = stock -1").
                eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0)
                .update();
        if(!success){
            log.error("库存不足");
        }

    }
}
