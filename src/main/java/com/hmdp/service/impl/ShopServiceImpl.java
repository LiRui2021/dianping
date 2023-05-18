package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
  private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
       //Shop shop = queryWithPassThrough(id);
       Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
       // Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicExpire(id);
      // Shop shop = cacheClient.queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        //返回
        return Result.ok(shop);
    }
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        //从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否存在
        //存在，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if(shopJson != null){
            return null;
        }
        //4.实现缓存重建
        //4.1 获取互斥锁
        //4.2 判断是否获取成功
        //4.3 失败，则休眠病充实
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            if(!isLock){
                Thread.sleep(50);
               return queryWithMutex(id);
            }
            //4.4成功，根据id差
            //不存在
            //1.查数据库
            shop = getById(id);
            //模拟重建的延时
            Thread.sleep(200);
            //2.判断商铺是否存在
            //不存在，返回错误
            if(shop == null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //3.如果存在，写入Redis，返回数据
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(lockKey);
        }
        return shop;

    }
//    public Shop queryWithPassThrough(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        //从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断缓存是否存在
//        //存在，直接返回
//        if(StrUtil.isNotBlank(shopJson)){
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
//        if(shopJson != null){
//            return null;
//        }
//        //不存在
//        //1.查数据库
//        Shop shop = getById(id);
//        //2.判断商铺是否存在
//        //不存在，返回错误
//        if(shop == null){
//            //将空值写入redis
//            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//        //3.如果存在，写入Redis，返回数据
//        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        return shop;
//
//    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    public Shop queryWithLogicExpire(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        //从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断缓存是否存在
//        ////不存在，直接返回空
//        if(StrUtil.isBlank(shopJson)) {
//        return null;
//        }
//        //命中，需要先把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        JSONObject jsonObject = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(jsonObject, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //5.判断是否过期
//        //未过期，返回店铺信息
//       if(expireTime.isAfter(LocalDateTime.now())){
//           return shop;
//       }
//        //过期，需要缓存重建
//        //获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean tryLock = tryLock(lockKey);
//        //判断时否获取成功
//        //成功，开启独立线程，实现缓存重建
//        if(tryLock){
//           new Thread(){
//               @Override
//               public void run() {
//                   try {
//                       saveShop2Redis(id,100L);
//                   } catch (Exception exception) {
//                       exception.printStackTrace();
//                   } finally {
//                       unlock(lockKey);
//                   }
//               }
//           }.start();
//        }
//
//        //返回过期商铺信息
//
//        return shop;
//
//    }

     private boolean tryLock(String key){
         Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
         return BooleanUtil.isTrue(flag);
     }

      private void unlock(String key){
        stringRedisTemplate.delete(key);
      }
//
//      public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
//          //1.查询店铺数据
//          Shop shop = getById(id);
//          Thread.sleep(200);
//          //2.封装逻辑过期时间
//          RedisData redisData = new RedisData();
//          redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//          redisData.setData(shop);
//          //3.写入redis
//          stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
//      }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
         //判断是否需要根据坐标查询
           if(x==null || y==null){
               Page<Shop> page = query()
                       .eq("type_id", typeId)
                       .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
               // 返回数据
               return Result.ok(page.getRecords());
           }
        //2.计算分页参数
           int from = (current-1) * SystemConstants.DEFAULT_PAGE_SIZE;
           int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis，按照距离排序丶分页
           //GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000), RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        if(results==null){
            return Result.ok();
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> geoResultList = results.getContent();
        if(geoResultList.size() <= from){
            return  Result.ok();
        }
       //4.1截取from - end 的bufen
        List<Long> shops = new ArrayList<>();
        Map<String,Distance> map = new HashMap<>() ;
        geoResultList.stream().skip(from).forEach(result ->{
            //4.2获取店铺id
            String shopId = result.getContent().getName();
            shops.add(Long.valueOf(shopId));
            //4.3获取距离
            Distance distance = result.getDistance();
            map.put(shopId,distance);
        } );
        String idstr = StrUtil.join(",",shops);
        //根据id查询shop
        List<Shop> shopList = query().in("id", shops).last("order by field(id," + idstr + ")").list();
       for(Shop shop : shopList){
           shop.setDistance(map.get(shop.getId().toString()).getValue());
       }
      return Result.ok(shopList);
    }
}
