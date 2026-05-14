package com.example.stockconsumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import tools.jackson.databind.ObjectMapper;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory,
                                                        ObjectMapper objectMapper) {

        // 레디스 템플릿 객체 만들기
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory); // 레디스 연결 정보 넣어주기

        // Redis 의 Key 는 문자열로 직렬화 할 수 있도록 설정
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer()); // Hash Key 도 사용가능

        // Redis 의 Value 는 JSON 형태로 직렬화 할 수 있도록 설정
        GenericJacksonJsonRedisSerializer serializer = new GenericJacksonJsonRedisSerializer(objectMapper);
        redisTemplate.setValueSerializer(serializer);
        redisTemplate.setHashValueSerializer(serializer); // Hash Key 도 사용가능

        redisTemplate.afterPropertiesSet(); // 위에서 레디스 연결 정보, Key, Value 설정 넣어주고 적용시키는 단계

        return redisTemplate;
    }

}
