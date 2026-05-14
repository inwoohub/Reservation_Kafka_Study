package com.example.stockconsumer.kafka.dto;

import com.example.stockconsumer.domain.product.ProductStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private Integer price;

    private Integer stock;

    @Enumerated(EnumType.STRING)
    private ProductStatus status;

    private LocalDateTime concertDateTime;

}
