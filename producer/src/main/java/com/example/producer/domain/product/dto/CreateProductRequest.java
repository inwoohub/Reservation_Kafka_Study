package com.example.producer.domain.product.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateProductRequest {

    private String name;

    private Integer price;

    private Integer stock;

    private LocalDateTime concertDateTime;

}
