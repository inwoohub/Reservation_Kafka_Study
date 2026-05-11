package com.example.stockconsumer.domain.product;

import com.example.stockconsumer.kafka.dto.Reservation;
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
    @Transactional
    public void stockService(Reservation event) {

        // 1. 상품 번호로 물건 가져오기
        Product product = productRepository.findById(event.getProductId())
                .orElseThrow(() -> new IllegalArgumentException("상품을 찾을 수 없습니다. : " + event.getProductId()));

        // 2. 구매자 수량 비교하기
        if(product.getStock() < event.getQuantity()){
            throw new IllegalArgumentException("재고보다 주문하려는 수량이 더 많습니다.");
        }

        // 3. 상품 차감시키기
        product.decreaseStock(event.getQuantity());

    }



}
