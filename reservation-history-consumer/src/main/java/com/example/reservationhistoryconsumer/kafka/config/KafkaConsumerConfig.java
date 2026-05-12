package com.example.reservationhistoryconsumer.kafka.config;

import com.example.reservationhistoryconsumer.kafka.dto.KafkaEventReservation;
import com.example.reservationhistoryconsumer.kafka.dto.KafkaEventStockResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    /**
     * yml 을 통해 1개의 토픽만 지정해서 구독을 하는 상황에서
     * 2개의 토픽을 리스너로 등록해야함.
     * 그래서 다른 DTO 도 읽어 올 수 있도록 Bean 으로 등록해줌
     */

    /**
     * reservationConsumerFactory
     * 구독 리스너의 메시지 DTO -> KafkaEventReservation
     * 1. 역직렬화 하고 싶은 Class 넣어서 deserializer 만들기
     * 2. 보안 설정하기 여기서는 2개함, 패키지 다 열어두기, 헤더 타입 무시하기
     * 3. props 만들기 -> Consumer 만들 때 필요한 것
     *    ex) 그룹id , earliest, 서버 주소
     * 4. return으로 DefaultFactory 넘기기
     */
    @Bean
    public ConsumerFactory<String, KafkaEventReservation> reservationConsumerFactory() {
        JacksonJsonDeserializer<KafkaEventReservation> deserializer =
                new JacksonJsonDeserializer<>(KafkaEventReservation.class);
        // new Jac~~<>(KafkaEventReservation.class) 를 통해서 직접 넣고 싶은 class를 넣음으로 DTO 역직렬화 가능하게끔 만드는것임.

        deserializer.trustedPackages("*"); // 모든 경로 신뢰 - 보안 관련된 설정임.
        // 지금은 학습용이지만 나중에 보안을 생각하면
        // deserializer.trustedPackages("com.example.reservationhistoryconsumer.kafka.dto") 처럼 1개만 지정 가능함.

        deserializer.ignoreTypeHeaders();  // 헤더 타입 무시하기
        // __TypeId__ = com.example.producer.kafka.dto.KafkaEventReservation 이 타입ID 무시 하는것임.
        // 이거 없으면 저 타입 경로 없는데 쫓아가서 저번에 났었던 SerializationException 이 발생함! (몇개 더 있음)

        Map<String, Object> props = commonConsumerProps();
        // Consumer 를 만들 때 필요한 기본 설정들 가져오는것
        // Map 으로 만들어서 나중에 DefaultKafkaConsumerFactory 를 만들 때 필요한 재료

        return new DefaultKafkaConsumerFactory<>(
                props,                    // Consumer 만들 때 기본 설정들
                new StringDeserializer(), // String 으로 역직렬화 객체?
                deserializer              // 이건 DTO 역직렬화 할 수게 만든 것
        );
    }


    /**
     * Bean으로 등록해둔, reservationConsumerFactory 를 넣음으로써
     * @KafkaListener가 실행될 때 해당 ConsumerFactory 를 사용해서 Kafka Consumer를 만들고
     * 그 Consumer 로 메시지를 읽게 시킴
     *
     * @param reservationConsumerFactory
     * @return ConcurrentKafkaListenerContainerFactory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaEventReservation> reservationKafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaEventReservation> reservationConsumerFactory // Param 으로 reservationConsumerFactory 넣어줌
    ) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaEventReservation> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(reservationConsumerFactory);
        return factory;
    }


    /**
     * stockResultConsumerFactory
     * 구독 리스너의 메시지 DTO -> KafkaEventStockResult
     * 1. 역직렬화 하고 싶은 Class 넣어서 deserializer 만들기
     * 2. 보안 설정하기 여기서는 2개함, 패키지 다 열어두기, 헤더 타입 무시하기
     * 3. props 만들기 -> Consumer 만들 때 필요한 것
     *    ex) 그룹id , earliest, 서버 주소
     * 4. return으로 DefaultFactory 넘기기
     *
     * -> 가장 위에있던거 KafkaEventStockResult 로 바꾼 버전이라고 생각하면됨.
     */
    @Bean
    public ConsumerFactory<String, KafkaEventStockResult> stockResultConsumerFactory() {
        JacksonJsonDeserializer<KafkaEventStockResult> deserializer =
                new JacksonJsonDeserializer<>(KafkaEventStockResult.class);

        deserializer.trustedPackages("*");
        deserializer.ignoreTypeHeaders();

        Map<String, Object> props = commonConsumerProps();

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer
        );
    }


    /**
     * 이것도 Kafka Consumer 만들어 놓고
     * 리스너로 가져다 쓰기 위한 것임
     * @param stockResultConsumerFactory
     * @return factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaEventStockResult> stockResultKafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaEventStockResult> stockResultConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaEventStockResult> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(stockResultConsumerFactory);
        return factory;
    }


    /**
     * Consumer 만들 때 넣어줘야하는 Props
     * @return props
     */
    private Map<String, Object> commonConsumerProps() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reservation-history-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

}