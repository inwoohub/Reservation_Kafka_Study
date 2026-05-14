package com.example.producer.domain.reservation;

import com.example.producer.domain.product.Product;
import com.example.producer.domain.product.ProductRepository;
import com.example.producer.domain.product.ProductStatus;
import com.example.producer.domain.reservation.dto.CreateReservationRequest;
import com.example.producer.domain.reservation.dto.KafkaEventReservation;
import com.example.producer.domain.reservation.dto.ReservationStatus;
import com.example.producer.global.error.ApiException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationService {

    private final KafkaTemplate<String, KafkaEventReservation> kafkaTemplate;
    private final ProductRepository productRepository;

    private final String CREATE_TOPIC = "reservation_requested";

    public void addReservation(CreateReservationRequest req) {

        // 1. 상품 id 통해 상품 가져오기
        Product product = productRepository.findById(req.getProductId())
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "404", "NOT_FOUND", "존재하지 않는 상품입니다."));

        // 2. 상품이 판매중인지 확인하기
        if(!product.getStatus().equals(ProductStatus.SELLING)){
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "판매중이지 않은 상품입니다.");
        }


        // 3. eventId & orderId UUID 타입
        String eventId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();

        Long timestamp = System.currentTimeMillis();

        // 4. 카프카에 올릴 이벤트 객체로 바꾸기
        KafkaEventReservation kafkaEventReservation = new KafkaEventReservation(eventId, orderId, req, product, ReservationStatus.PURCHASE_REQUESTED,timestamp);

        // 5. 카프카에 데이터 올리기
        kafkaTemplate.send(CREATE_TOPIC, kafkaEventReservation);


    }

}
