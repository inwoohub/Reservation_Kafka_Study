package com.example.stockconsumer;

import com.example.stockconsumer.kafka.StockConsumer;
import com.example.stockconsumer.kafka.dto.Reservation;
import com.example.stockconsumer.kafka.dto.ReservationStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDate;
import java.time.LocalDateTime;

@SpringBootTest
public class ConsumeTest {

    @Autowired
    private StockConsumer stockConsumer;

    @Test
    @DisplayName("읽어오고 나서 다시 올릴 때 어떯게 올리가는지 객체 확인하기")
    public void test1() throws Exception{

//        Reservation reservation = new Reservation(1L, "eventId", "orderId", 3L, 3, 192000, "뚱이", "000217", "0217", ReservationStatus.PURCHASE_CONFIRMED, 17712382L ,LocalDateTime.now());

//        stockConsumer.reservationSuccess(reservation);

    }


}
