package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Autowired
    private IBlogService blogService;
    @Resource
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService iFollowService;
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            queryBlogUser(blog);
            isLikedBlog(blog);
        });
        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("笔记不存在");
        }
        queryBlogUser(blog);
        //查询是否被当前用户点赞
        isLikedBlog(blog);
        return Result.ok(blog);
    }

    private void isLikedBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if(user==null){
            return;
        }
        Long userId = user.getId();
        //2.判断是否已经点赞
        String key = "blog:liked:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score!=null);

    }

    @Override

    public Result likeBlog(Long id) {
        UserDTO user = UserHolder.getUser();
        if(user==null){
            return Result.fail("请先登录");
        }
        //1.获取登录用户
        Long userId = user.getId();
        //2.判断是否已经点赞
        String key = "blog:liked:" + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //2.1无，存入点赞信息到redis sortedset集合，数据库点赞信息加一
          if(score==null){
              boolean isSuccess = blogService.update().setSql("liked = liked + 1").eq("id", id).update();
             if(isSuccess){
                 stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
                 return Result.ok("点赞成功");
             }else
                 return Result.ok("点赞失败");

          }
        //2.2如果有点赞，数据库减一，删除redis中的点赞信息
          else{
              boolean isSuccess = blogService.update().setSql("liked = liked - 1").eq("id", id).update();
              if(isSuccess){
                  stringRedisTemplate.opsForZSet().remove(key,userId.toString());
                  return Result.ok("取消成功");
              }else
                  return Result.fail("取消失败");

          }
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //1.查询top5的点赞用户 zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY + id, 0, 4);
        //2.解析出其中的用户id
        //3.根据用户id查询用户
        List<UserDTO> userDTOS = top5.stream().map(item -> {
            Long userId = Long.valueOf(item);
            User user = userService.getById(userId);
            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            return userDTO;
        }).collect(Collectors.toList());
        //4.返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = blogService.save(blog);
        if(!isSuccess){
            return Result.fail("保存笔记失败");
        }
        //查询笔记作者的所有粉丝
        List<Follow> follows = iFollowService.query().eq("follow_user_id", blog.getUserId()).list();
        //4.推送笔记id给所有粉丝
        for(Follow follow : follows){
            Long userId = follow.getUserId();
            //4.2推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2.查询收件箱 ZREVRANGEBYSCORE key max imin limit offset count
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().
                reverseRangeByScoreWithScores(key, 0, max, offset, 2);

       //3.非空判断
        if(typedTuples==null || typedTuples.isEmpty()){
            return Result.ok();
        }
        //4.解析数据：blogId丶minTime（时间戳），offset
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        //偏移量
        int os = 1;
      for(ZSetOperations.TypedTuple<String> typedTuple : typedTuples){
          //推送的日志
          String blogId = typedTuple.getValue();
          blogIds.add(Long.valueOf(blogId));
          Long time = typedTuple.getScore().longValue();
          if(minTime == time){
              os++;
          }else{
              //时间戳
              minTime = time;
              os = 1;
          }


      }
        //5.根据id查询blog
        String idStr = StrUtil.join(",", blogIds);
        List<Blog> blogList = blogService.query().in("id", blogIds).last("order by field(id," + idStr + ")").list();
        blogList.forEach(blog->{
            queryBlogUser(blog);
            isLikedBlog(blog);
        });
        //6封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogList);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);
    }
}
