package com.example.stockconsumer.domain.product;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Table(name = "product")
@Entity
@Getter
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

    // 재고 감소 시키기
    public void decreaseStock(Integer stock) {
        this.stock = this.stock - stock;
    }

    // 재고 증가 시키기 (환불 경우)
    public void increaseStock(Integer stock) {
        this.stock = this.stock + stock;
    }

    // 품절 상태 변경
    public void setStatus(ProductStatus status) {
        this.status = status;
    }


}