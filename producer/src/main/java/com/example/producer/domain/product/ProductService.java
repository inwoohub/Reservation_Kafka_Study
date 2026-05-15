package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String PRODUCT_PREFIX = "product:";
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
    public Map<Object, Object> getSellingAllV2() {

        // 판매중인 제품 모두 가져오기 (레디스에 올라가 있다면 전부 판매중인 것)
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(PRODUCT_PREFIX);

        return entries;
    }

}
