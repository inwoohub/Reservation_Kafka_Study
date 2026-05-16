package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String PRODUCT_PREFIX = "product:info:";
    private static final String PRODUCT_STOCK_PREFIX = "product:stock:";

    @Transactional
    public void addProduct(CreateProductRequest req) {
        Product product = new Product(req);

        // 1. DB에 직접 상품 저장하기
        Product savedProduct = productRepository.save(product);

        // 2. Redis 에서도 저장해주기 -> 이걸 판매자한테 콘서트 호출 시 함께 보내줄예정
        HashMap<String, String> redisValue = new HashMap<>();
        redisValue.put("productId", savedProduct.getId().toString()); // 콘서트 ID
        redisValue.put("productName", savedProduct.getName());        // 콘서트 이름
        redisValue.put("concertDateTime", savedProduct.getConcertDateTime().toString()); // 콘서트 시간
        redisValue.put("price", savedProduct.getPrice().toString());  // 콘서트 가격
        redisValue.put("status", savedProduct.getStatus().toString());// 콘서트 판매 상태

        // 3. 상품에 대한 정보 저장하기
        redisTemplate.opsForValue().set(PRODUCT_PREFIX + savedProduct.getId(), redisValue);

        // 4. 재고만 저장하기
        redisTemplate.opsForValue().set(PRODUCT_STOCK_PREFIX + savedProduct.getId(), savedProduct.getStock());

    }


    // 모든 제품 조회
    public List<Product> getProducts() {
        return productRepository.findAll();
    }

    // 판매중인 제품만 조회
    public List<Product> getSellingAll() {
        return productRepository.getSellingAll();
    }


    // 판매중인 제품만 조회 (Redis 활용)
    public List<Map<String, Object>> getSellingAllV2() {

        Set<String> infoKeys = redisTemplate.keys(PRODUCT_PREFIX + "*");
        Set<String> stockKeys = redisTemplate.keys(PRODUCT_STOCK_PREFIX + "*");

        // 비어있는 경우 비어있는 리스트로 반환
        if (infoKeys.isEmpty() && stockKeys.isEmpty()) {
            return List.of();
        }

        // 반환용 빈 결과 리스트 생성
        List<Map<String, Object>> result = new ArrayList<>();

        // info 키를 기준으로 탐색시작
        for (String key : infoKeys) {

            // 물건 정보 가져오기
            Object productValue = redisTemplate.opsForValue().get(key);

            // productValue가 Map 타입이면 productMap이라는 이름으로 꺼내서 쓰고, Map 타입이 아니면 이번 반복은 건너뜀
            if (!(productValue instanceof Map<?, ?> productMap)) {
                continue;
            }

            // 맵 객체 하나 만듦
            Map<String, Object> product = new HashMap<>();

            // productMap 으로 꺼낸거 돌면서 하나씩 꺼내면서 product 에 넣어주기
            for (Map.Entry<?, ?> entry : productMap.entrySet()) {
                product.put(String.valueOf(entry.getKey()), entry.getValue());
            }

            // Id만 따로 꺼내서 스트링으로 변환
            String productId = String.valueOf(product.get("productId"));

            // Id 만 따로 꺼내서 수량도 꺼내오기
            Object stock = redisTemplate.opsForValue()
                    .get(PRODUCT_STOCK_PREFIX + productId);

            // 수량도 담아주기
            product.put("stock", stock);

            // 만든 Map 객체 리스트에 더하기
            result.add(product);
        }

        return result;

    }

}
