package com.example.stockconsumer.domain.product;

import com.example.stockconsumer.kafka.dto.KafkaEventReservationRequest;
import com.example.stockconsumer.kafka.dto.KafkaEventStockResult;
import com.example.stockconsumer.kafka.dto.Reservation;
import com.example.stockconsumer.kafka.dto.ReservationStatus;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.NoSuchElementException;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ProductService {

    private final KafkaTemplate<String, KafkaEventStockResult> kafkaTemplateV2;

    private final String STOCK_TOPIC = "stock-result";

    private final ProductRepository productRepository;

    private final EntityManager entityManager;

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

    @Transactional
    public boolean stockIncrease(KafkaEventReservationRequest event) {
        // 1. 구매자 수량 확인하기
        if (event.getQuantity() == null || event.getQuantity() <= 0) {
            log.info("구매자 주문 수량이 음수거나 null값 입니다.");
            return false;
        }

        // 2. 재고 업데이트 원자 처리
        boolean decreaseStockCheck = productRepository.increaseStock(
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

    /**
     * 해당 감소 하는 과정에서 동시성 문제로 인해 데이터 정합성이 안 맞았음
     * 증상 :
     *  재고가 2개 남았는데 1개 수량이 빠져나가 재고가 1개 남은 상황에서
     *  판매상태가 닫힘으로 바뀌는 현상
     *
     * 문제 의심 :
     *  Redis 도 정상적으로, 로그를 통해서도 정상적으로 수량이 들어옴
     *  그렇다면 Update 하는 과정에서 SET 의 위치에 따라 연산이 다르게 되는지 의심됨
     *  long updateCount = jpaQueryFactory
     *                 .update(product)
     *                 .set(product.stock, product.stock.subtract(quantity)) 선 감소!
     *                 .set(
     *                         product.status, new CaseBuilder()
     *                                 .when(product.stock.subtract(quantity).eq(0))
     *                                 .then(ProductStatus.CLOSED)
     *                                 .otherwise((product.status))
     *                 )
     *  위에 쿼리와 같이 먼저 수량만큼 감소하고 그 다음에 수량을 비교해서 상태를 업데이트 함.
     *  원하는 동작 -> 하나의 스냅샷을 따와서 그 스냅샷에서 재고 먼저 감소 처리
     *              그 후 그 스냅샷에 이어서 또 다른 재고 빼고났을 떄 재고 비교 후 상태 변동
     *              한번에 커밋을 통해 UPDATE 처리
     *
     *  실제 동작 -> 하나의 UPDATE 문 안에서 앞선 SET 결과를 뒤의 SET 표현식이 참조한 것으로 보임
     *             실제로는 -1 만 했지만 비교까지 들어가서 -2 가 들어감으로 상태 변경이 이루어 졌던것이다.
     *
     *  해결 방법 :
     *   상태를 관련해서 먼저 연산하는 set의 위치를 앞단으로 빼고
     *   실제 재고를 차단하는 set을 후순위로 미루었다.
     *   long updateCount = jpaQueryFactory
     *                 .update(product)
     *                 .set(
     *                         product.status, new CaseBuilder()
     *                                 .when(product.stock.subtract(quantity).eq(0))
     *                                 .then(ProductStatus.CLOSED)
     *                                 .otherwise((product.status))
     *                 )
     *                 .set(product.stock, product.stock.subtract(quantity)) 후 감소!
     */
    @Transactional
    public boolean stockDecrease(KafkaEventReservationRequest event) {

//        Product beforeProduct = productRepository.findById(event.getProductId())
//                .orElseThrow(() -> new NoSuchElementException("없슴"));
//
//        log.info("MySQL 재고 감소 전 productId={}, beforeStock={}, beforeStatus={}, quantity={}",
//
//                event.getProductId(),
//
//                beforeProduct.getStock(),
//
//                beforeProduct.getStatus(),

//                event.getQuantity());

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



//        entityManager.flush();
//        entityManager.clear();

//        Product afterProduct = productRepository.findById(event.getProductId())
//                .orElse(null);
//
//        if (afterProduct != null) {
//
//            log.info("MySQL 재고 감소 후 productId={}, afterStock={}, afterStatus={}, updateResult={}",
//
//                    event.getProductId(),
//
//                    afterProduct.getStock(),
//
//                    afterProduct.getStatus(),
//
//                    decreaseStockCheck);
//
//        }

        return true;
    }


}
