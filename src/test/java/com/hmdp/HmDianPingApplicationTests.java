package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Test
    void testSaveShop() throws InterruptedException {
        //shopService.saveShop2Redis(1L,100L);
    }
    @Test
    void loadShopDate(){
        //1.查询店铺信息
        List<Shop> list = shopService.list();
        //2.把店铺分组，按照typeId分组，typeId一直的放到一个集合
        Map<Long, List<Shop>> listMap = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //分组完成写入redis
        for(Map.Entry<Long,List<Shop> >entry : listMap.entrySet()){
            //3.1获取类型id
            Long key = entry.getKey();
            //3.2获取同类新的店铺的集合
            List<Shop> shops = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
            for(Shop shop : shops){
                Point point = new Point(shop.getX(), shop.getY());
                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),point));
            }
            //3.3写入redis GEOADD key 精度 维度 member
           stringRedisTemplate.opsForGeo().add(SHOP_GEO_KEY + key,locations);
        }
    }
}
