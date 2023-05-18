package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        //获取请求头中的token
        String token = request.getHeader("authorization");
        //获取redis中的用户
        if(StrUtil.isBlank(token)){
            //response.setStatus(401);
            return true;
        }
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries( token);

        //3.判断用户是否存在
        //4.不存在，拦截
        if(userMap.isEmpty()){
            //response.setStatus(401);
           return true;
        }
        //5.将查询到的Hash数据转为UserDTO对象
        UserDTO user = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        //6.存在，保存用户信息到ThreadLocal
        UserHolder.saveUser(user);
        //7.刷新redis有效期
        stringRedisTemplate.expire(token,300, TimeUnit.MINUTES);
        //6.放行
        return true;
    }




    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
