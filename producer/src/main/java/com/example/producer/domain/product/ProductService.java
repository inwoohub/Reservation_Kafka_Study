package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;

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

        // 2. Redis 에서도 저장해주기
        HashMap<String, String> redisValue = new HashMap<>();
        redisValue.put("productId", savedProduct.getId().toString());
        redisValue.put("price", savedProduct.getPrice().toString());
        redisValue.put("status", savedProduct.getStatus().toString());

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

}
