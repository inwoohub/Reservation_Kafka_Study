package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    private final RedisTemplate<String, Object> redisTemplate;

    private final String PREFIX = "product:stock:";

    @Transactional
    public void addProduct(CreateProductRequest req) {
        Product product = new Product(req);

        // 1. DB에 직접 상품 저장하기
        Product savedProduct = productRepository.save(product);

        // 2. Redis 에서도 저장해주기
        redisTemplate.opsForValue().set(PREFIX+savedProduct.getId() ,req.getStock() );

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
