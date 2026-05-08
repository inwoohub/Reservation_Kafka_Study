package com.example.producer.domain.reservation;

import com.example.producer.domain.product.Product;
import com.example.producer.domain.product.ProductRepository;
import com.example.producer.domain.reservation.dto.CreateReservationRequest;
import com.example.producer.domain.reservation.dto.KafkaEventReservation;
import com.example.producer.domain.reservation.dto.ReservationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProductRepository productRepository;

    private final String CREATE_TOPIC = "create-reservation";

    public void addReservation(CreateReservationRequest req) {

        // 1. 상품 id 통해 상품 가져오기
        Product product = productRepository.findById(req.getProductId())
                .orElseThrow(() -> new NullPointerException("Product not found"));

        String eventId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();

        Long timestamp = System.currentTimeMillis();

        // 2. 카프카에 올릴 이벤트 객체로 바꾸기
        KafkaEventReservation kafkaEventReservation = new KafkaEventReservation(eventId, orderId,req, product, ReservationStatus.PURCHASE_REQUESTED,timestamp);

        // 3. 카프카에 데이터 올리기
        kafkaTemplate.send(CREATE_TOPIC, kafkaEventReservation.toString());

    }

}
