package com.example.producer.domain.reservation;

import com.example.producer.domain.product.Product;
import com.example.producer.domain.product.ProductRepository;
import com.example.producer.domain.product.ProductStatus;
import com.example.producer.domain.reservation.dto.CancelReservationRequest;
import com.example.producer.domain.reservation.dto.CreateReservationRequest;
import com.example.producer.domain.reservation.dto.GetReservationRequest;
import com.example.producer.kafka.dto.KafkaEventReservation;
import com.example.producer.kafka.dto.KafkaEventReservationCancel;
import com.example.producer.kafka.dto.KafkaEventReservationRequest;
import com.example.producer.domain.reservation.dto.ReservationStatus;
import com.example.producer.global.error.ApiException;
import com.example.producer.kafka.dto.KafkaEventStockResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ReservationService {

    private final KafkaTemplate<String, KafkaEventReservation> kafkaTemplate;
    private final KafkaTemplate<String, KafkaEventReservationRequest> kafkaTemplateV2;

    private final ProductRepository productRepository;

    private final String REQUEST_TOPIC = "reservation_requested";

    private final RedisTemplate<String, Object> redisTemplate;

    private final RedisScript<Long> decreaseStockScript; // RedisConfig 에서 Bean으로 만든 스크립트 주입 받기

    private static final String PRODUCT_PREFIX = "product:info:";
    private static final String PRODUCT_STOCK_PREFIX = "product:stock:";
    private final ReservationRepository reservationRepository;

    // 기존 버전
    public void addReservation(CreateReservationRequest req) {

        // 1. 상품 id 통해 상품 가져오기
        Product product = productRepository.findById(req.getProductId())
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "404", "NOT_FOUND", "존재하지 않는 상품입니다."));

        // 2. 상품이 판매중인지 확인하기
        if (!product.getStatus().equals(ProductStatus.SELLING)) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "판매중이지 않은 상품입니다.");
        }

        // 3. eventId & orderId UUID 타입
        String eventId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();

        Long timestamp = System.currentTimeMillis();

        // 4. 카프카에 올릴 이벤트 객체로 바꾸기
        KafkaEventReservation kafkaEventReservation = new KafkaEventReservation(eventId, orderId, req, product, ReservationStatus.PURCHASE_REQUESTED, timestamp);

        // 5. 카프카에 데이터 올리기
        kafkaTemplate.send(REQUEST_TOPIC, kafkaEventReservation);
    }

    /**
     * 레디스를 활용한 성능 개선버전
     * <p>
     * 기존 : MySQL - SELECT 1회 후 카프카 이벤트 로그 올리기
     * 개선 : Redis - Get 1회 후 카프카 이벤트 로그 올리기
     * <p>
     * 차이 : MySQL 연산 속도보다 비교적 가벼운 Redis 를 통해서 재고 확인
     */
    public void addReservationV2(CreateReservationRequest req) {

        // 1. 제품 수량 및 제품 정보 꺼내오기
        Integer stock = (Integer) redisTemplate.opsForValue().get(PRODUCT_STOCK_PREFIX + req.getProductId());
        Object productValue = redisTemplate.opsForValue().get(PRODUCT_PREFIX + req.getProductId());

        // 2. 제품이 없거나 판매 종료 시 예외 처리
        if (stock == null || productValue == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "404", "NOT_FOUND", "존재하지 않는 상품입니다.");
        }

        if (stock <= 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "품절된 상품 입니다.");
        }

        if (req.getQuantity() > stock) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "재고보다 주문 수량이 많습니다.");
        }

        // null 이 아니니까 타입 변환하기
        Map<String, String> productInfo = (Map<String, String>) productValue;

        // 3. eventId & orderId UUID 타입 & timestamp 생성
        String eventId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();
        Long timestamp = System.currentTimeMillis();

        // 4. 카프카에 올릴 이벤트 객체로 바꾸기
        int price = Integer.parseInt(productInfo.get("price"));
        ProductStatus productStatus = ProductStatus.valueOf(productInfo.get("status"));
        KafkaEventReservation kafkaEventReservation = new KafkaEventReservation(
                eventId,
                orderId,
                req,
                price,
                productStatus,
                ReservationStatus.PURCHASE_REQUESTED,
                timestamp);

        // 5. 카프카에 데이터 올리기
        kafkaTemplate.send(REQUEST_TOPIC, kafkaEventReservation);
    }


    /**
     * 기존 예매 내역에 대한 저장을 해당 서비스로 역할을 바꾸면서
     * 예매 관리 서비스라는 역할을 부여함.
     * <p>
     * 1. 레디스 재고 확인 및 감소
     * 2. 예매 내역 생성 및 저장
     * 3. 예매 내역 이벤트 발행 (2번 성공 시 에만)
     * 4. 예매 결과 반환
     */
    @Transactional
    public boolean addReservationV3(CreateReservationRequest req) {

        // 1. 제품 정보 꺼내오기
        Object productValue = redisTemplate.opsForValue().get(PRODUCT_PREFIX + req.getProductId());

        // eventId & orderId UUID 타입 & timestamp 생성
        String eventId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();
        Long timestamp = System.currentTimeMillis();

        // null 이 아니니까 타입 변환하기
        Map<String, String> productInfo = (Map<String, String>) productValue;
        int totalPrice = Integer.parseInt(productInfo.get("price")) * req.getQuantity();

        // 2. Redis 에서 재고 확인 및 감소 처리
        Long stockServiceCheck = redisTemplate.execute(
                decreaseStockScript,
                List.of(PRODUCT_STOCK_PREFIX + req.getProductId()),
                req.getQuantity()
        );

        // 2. 예매 내역 생성 및 저장
        // 2-1. 재고 자체가 없음(상품 등록x) : -1
        if (stockServiceCheck == -1L) {
            // 예약 실패 저장
            Reservation reservation = new Reservation(eventId, orderId, req, ReservationStatus.PURCHASE_FAILED, totalPrice, timestamp);
            reservationRepository.save(reservation);
            log.info("addReservationV3 Error : 상품이 존재하지 않음 !!");
            return false;
        }

        // 2-2. 재고 보다 주문 수량이 많이 들어옴  : 0
        else if (stockServiceCheck == 0L) {
            // 예약 실패 저장
            Reservation reservation = new Reservation(eventId, orderId, req, ReservationStatus.PURCHASE_FAILED, totalPrice, timestamp);
            reservationRepository.save(reservation);
            log.info("addReservationV3 Error : 재고보다 주문 수량이 많음 !!");
            return false;
        }

        // 2-3. 성공 : 1
        else if (stockServiceCheck == 1L) {
            // 예약 요청 성공 저장
            Reservation reservation = new Reservation(eventId, orderId, req, ReservationStatus.PURCHASE_REQUESTED, totalPrice, timestamp);
            Reservation saved = reservationRepository.save(reservation);

            // 3. 예매 요청 이벤트 발행
            KafkaEventReservationRequest kafkaEventReservationRequest = new KafkaEventReservationRequest(saved.getId(), saved.getProductId(), saved.getQuantity(), ReservationStatus.PURCHASE_REQUESTED);
            kafkaTemplateV2.send(REQUEST_TOPIC, kafkaEventReservationRequest);

        }

        // 4. 예매 결과 반환
        return true;
    }

    @Transactional
    public boolean updateStatus(KafkaEventStockResult event) {

        // 업데이트 원자 처리
        return reservationRepository.setReservationStatus(event.getReservationId(), event.getReservationStatus());

    }

    // 해당 유저의 맞는 정보로 조회
    public List<Reservation> getAllReservations(GetReservationRequest req) {

        // 조회해서 반환
        List<Reservation> allReservation = reservationRepository.getAllReservation(req.getBuyerName(), req.getBirthDate(), req.getTeamPassword());

        if (allReservation.isEmpty()) {
            throw new ApiException(HttpStatus.NOT_FOUND, "404", "NOT_FOUND", "회원정보와 일치하는 예약 내역이 없습니다.");
        }

        return allReservation;

    }

    // 예약 취소하기
    public void cancelReservation(CancelReservationRequest req) {

        if(req.getId() == null){
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "reservationID 가 null 입니다.");
        }

        // 예약이 존재하는지 확인하기
        Reservation reservation = reservationRepository.findById(req.getId())
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "404", "NOT_FOUND", "존재하지 않는 예약 ID입니다."));

        // 예약 상태가 성공이었는지 확인하기
        if(!reservation.getReservationStatus().equals(ReservationStatus.CANCEL_COMPLETED)){
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "성공한 예약이 아님으로 취소할 수 없습니다.");
        }

        // 예약 비밀번호 검증하기
        if(!req.getTeamPassword().equals(reservation.getTempPassword())){
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "예약 시 사용했던 비밀번호가 틀렸습니다..");
        }

        // 예매 취소 신청으로 으로 으로 상태 담기
        KafkaEventReservationRequest kafkaEventReservationRequest = new KafkaEventReservationRequest(reservation.getId(), reservation.getProductId(), reservation.getQuantity(), ReservationStatus.CANCEL_REQUESTED);

        // 카프카 이벤트 발행하기
        kafkaTemplateV2.send(REQUEST_TOPIC, kafkaEventReservationRequest);

        // 예매할 떄는 바로 재고 차감 했지만 혹시모르니 재고 증가는 가장 마지막에 처리하기로!

    }

}
