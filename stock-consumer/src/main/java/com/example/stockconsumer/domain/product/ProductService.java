package com.example.stockconsumer.domain.product;

import com.example.stockconsumer.kafka.dto.KafkaEventReservationRequest;
import com.example.stockconsumer.kafka.dto.KafkaEventStockResult;
import com.example.stockconsumer.kafka.dto.Reservation;
import com.example.stockconsumer.kafka.dto.ReservationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ProductService {

    private final KafkaTemplate<String, KafkaEventStockResult> kafkaTemplateV2;

    private final String STOCK_TOPIC = "stock-result";

    private final ProductRepository productRepository;

    // 재고 확인 및 차감하는 서비스 필요함
    @Transactional
    public boolean stockService(Reservation event) {

        // 1. 상품 번호로 물건 가져오기
        Product product = productRepository.findById(event.getProductId()).orElse(null);

        // 2. 상품이 없는 경우 재고 차감전에 실패
        if (product == null) {
            log.info("stockService : 상품을 찾을 수 없습니다.");
            return false;
        }

        // 2. 구매자 수량 비교하기
        if (product.getStock() < event.getQuantity() || event.getQuantity() <= 0) {
            log.info("stockService : 구매 수량보다 재고가 부족합니다.");
            return false;
        }

        // 3. 상품 차감시키기
        product.decreaseStock(event.getQuantity());

        // 4. 만약 상품이 품절이 나면 품절 상태로 변경하기
        if (product.getStock() == 0) {
            product.setStatus(ProductStatus.CLOSED);
        }

        return true;
    }

    // 재고 확인 및 차감 원자적 처리
    @Transactional
    public boolean stockServiceV2(Reservation event) {

        // 1. 구매자 수량 확인하기
        if (event.getQuantity() == null || event.getQuantity() <= 0) {
            log.info("구매자 주문 수량이 음수거나 null값 입니다.");
            return false;
        }

        boolean decreaseStockCheck = productRepository.decreaseStock(
                event.getProductId(),
                event.getQuantity(),
                ProductStatus.SELLING,
                ProductStatus.CLOSED
        );

        if (!decreaseStockCheck) {
            log.info("재고가 부족하여 주문에 실패하였습니다.");
            return false;
        }
        return true;
    }

    /**
     * V3.
     * 버전 2에서 MySQL를 통해 재고를 처리하던 방식에서
     * Redis 에서 재고 처리하는 방식으로 변경
     */
    @Transactional
    public boolean stockServiceV3(Reservation event) {

        // 1. 구매자 수량 확인하기
        if (event.getQuantity() == null || event.getQuantity() <= 0) {
            log.info("구매자 주문 수량이 음수거나 null값 입니다.");
            return false;
        }

        boolean decreaseStockCheck = productRepository.decreaseStock(
                event.getProductId(),
                event.getQuantity(),
                ProductStatus.SELLING,
                ProductStatus.CLOSED
        );

        if (!decreaseStockCheck) {
            log.info("재고가 부족하여 주문에 실패하였습니다.");
            return false;
        }


        return true;
    }

    @Transactional
    public boolean stockServiceV4(KafkaEventReservationRequest event) {
        // 1. 구매자 수량 확인하기
        if (event.getQuantity() == null || event.getQuantity() <= 0) {
            log.info("구매자 주문 수량이 음수거나 null값 입니다.");
            return false;
        }

        // 2. 재고 업데이트 원자 처리
        boolean decreaseStockCheck = productRepository.decreaseStock(
                event.getProductId(),
                event.getQuantity(),
                ProductStatus.SELLING,
                ProductStatus.CLOSED
        );

        if (!decreaseStockCheck) {
            log.info("MySQL 재고 연산 과정에서 오류 발생했습니다.");
            return false;
        }

        return true;
    }


}
