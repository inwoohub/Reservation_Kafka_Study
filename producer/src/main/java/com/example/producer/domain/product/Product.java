package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Table(name = "product")
@Entity @Getter
@AllArgsConstructor
@NoArgsConstructor
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private Integer price;

    private Integer stock;

    private LocalDateTime concertDateTime;


    public Product(CreateProductRequest req) {
        this.name = req.getName();
        this.price = req.getPrice();
        this.stock = req.getStock();
        this.concertDateTime = req.getConcertDateTime();
    }


}
