package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryAll() {
        String key = CACHE_SHOP_TYPE_KEY;
        //从缓存中获取
        String shopType = stringRedisTemplate.opsForValue().get(key);
        //如果不为空直接返回
        if(StrUtil.isNotBlank(shopType)){
            return Result.ok(JSONUtil.toList(shopType,ShopType.class));
        }
        //如果为空
        //1.查询数据库
        List<ShopType> list = query().list();
        //如果为空直接返回false
        if(list.isEmpty()){
            return Result.fail("无此数据");
        }else{
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(list));
        }
        return Result.ok(list);
    }
}
