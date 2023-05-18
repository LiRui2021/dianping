package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
     @Resource
    StringRedisTemplate stringRedisTemplate;

    String key = "follow:";
    @Resource
    IUserService userService;
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //判断是否是关注还是取关
        //1.如果是关注
        UserDTO user = UserHolder.getUser();
        if(isFollow){
             Follow follow = new Follow();
             follow.setCreateTime(LocalDateTime.now());
             follow.setUserId(user.getId());
             follow.setFollowUserId(followUserId);
            boolean isSuccess = this.save(follow);
            if(isSuccess){
            stringRedisTemplate.opsForSet().add(key + user.getId(),followUserId.toString());
            }
            //2.如果是取关
         }else{
             LambdaUpdateWrapper<Follow> updateWrapper = new LambdaUpdateWrapper<>();
             updateWrapper.eq(Follow::getFollowUserId,followUserId).eq(Follow::getUserId,user.getId());
            boolean isSuccess = this.remove(updateWrapper);
            if(isSuccess)
            stringRedisTemplate.opsForSet().remove(key+user.getId(),followUserId.toString());
         }
       return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        //查询数据库，判断是否关注
        LambdaQueryWrapper<Follow> queryWrapper = new LambdaQueryWrapper<>();
        UserDTO user = UserHolder.getUser();
        queryWrapper.eq(Follow::getUserId,user.getId()).eq(Follow::getFollowUserId,followUserId);
        Follow follow = this.getOne(queryWrapper);
      return Result.ok(follow!=null);
    }

    @Override
    public Result commonFollow(Long id) {
        //查询当前用户id
        Long userId = UserHolder.getUser().getId();
        String k1 = key + userId;
        String k2 = key + id;
        //找出相同关注
        Set<String> common = stringRedisTemplate.opsForSet().intersect(k1, k2);
        if(common==null||common.isEmpty())
            return Result.ok(Collections.emptyList());
        List<Long> ids = common.stream().map(Long::valueOf).collect(Collectors.toList());
        List<User> users = userService.listByIds(ids);
        List<UserDTO> userDTOS = users.stream().map(item ->
            BeanUtil.copyProperties(item, UserDTO.class)
        ).collect(Collectors.toList());
        return Result.ok(userDTOS);
    }


}
