package com.example.producer.global.redis;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class RedisScriptConfig {

    /**
     * Redis에 저장된 재고를 감소시키는 Lua Script
     *
     * 1. Redis에서 현재 재고 조회 -> redis.call('GET', KEYS[1])
     *    ex) redisTemplate.execute( decreaseStockScript, List.of("product:stock:1"), "3" )
     *        처럼 실행 시 (스크립트명, 키로 들어갈 수 있는 리스트, 사용할 변수) 이렇게 넣으면 됨
     *
     * 2. 재고 키가 없으면 -1 반환 (판매 등록된 상품이 아님)
     *
     * 3. tonumber(stock) 요청 수량을 숫자로 변환
     *
     * 4. 현재 재고가 주문 수량보다 적다면 0 반환 (재고 부족)
     *
     * 5. quantity 만큼 감소 후 1 반환
     *
     * -1 : 재고 없음
     *  0 : 재고보다 주문 수량이 많음
     *  1 : 주문 성공
     */
    @Bean
    public RedisScript<Long> decreaseStockScript() {
        String lua = """
                local stock = redis.call('GET', KEYS[1])
                
                if not stock then
                    return -1
                end
                
                stock = tonumber(stock)
                local quantity = tonumber(ARGV[1])
                
                if stock < quantity then
                    return 0
                end
                
                redis.call('DECRBY', KEYS[1], quantity)
                return 1
            """;
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(lua);
        script.setResultType(Long.class);
        return script;
    }


}
