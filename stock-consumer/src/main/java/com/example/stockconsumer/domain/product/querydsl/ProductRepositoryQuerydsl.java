package com.example.stockconsumer.domain.product.querydsl;


import com.example.stockconsumer.domain.product.ProductStatus;

public interface ProductRepositoryQuerydsl {

    boolean decreaseStock(Long productId, Integer quantity, ProductStatus selling, ProductStatus closed);

}
