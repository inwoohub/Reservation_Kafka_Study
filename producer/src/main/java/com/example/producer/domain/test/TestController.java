package com.example.producer.domain.test;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@Tag(name ="99. TestController", description = "테스트 API")
@Slf4j
@RestController("/test")
@RequiredArgsConstructor
public class TestController {

    /**
     * 여기서 따로 Bean 통해 등록하지 않았더라도,
     * yml에서 설정했기 때문에 바로 가져다 쓰기가 가능하다.
     * 카프카도 결국 Key-Value 로 저장이 되는듯하다.
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 카프카에서는 TOPIC을 기준으로 데이터를 쌓는다
     * 그리고 TOPIC 을 기준으로 데이터를 읽기 때문에 TOPIC 은 하나의 카프카 내부에 카테고리인듯하다.
     */
    private static final String TOPIC = "test";

    @PostMapping("/sendMessage")
    public String sendTestMessage(@RequestBody TestRequestDTO req) {

        kafkaTemplate.send(TOPIC, req.toString());
        log.info("카프카 메시지 보내기 성공 ! {}", req.toString());
        return "카프카에서 메시지 확인해보쇼!";

    }


}
