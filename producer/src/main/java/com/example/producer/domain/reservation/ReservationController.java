package com.example.producer.domain.reservation;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "2. ReservationController", description = "콘서트 예약, 조회, 취소 API")
@RestController("/api/reservation")
@RequiredArgsConstructor
public class ReservationController {



}
