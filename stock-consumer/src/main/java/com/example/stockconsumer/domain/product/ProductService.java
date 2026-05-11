package com.example.stockconsumer.domain.product;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    // 재고 확인 및 차감하는 서비스 필요함
    public Void stockService(){

        // 1. 상품 번호로 물건 가져오기

        // 2. 구매자 수량 비교하기

        // 3. 상품 차감시키기

        return null;
    }



}
