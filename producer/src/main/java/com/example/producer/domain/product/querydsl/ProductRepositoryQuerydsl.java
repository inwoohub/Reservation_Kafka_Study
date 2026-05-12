package com.example.producer.domain.product.querydsl;

import com.example.producer.domain.product.Product;

import java.util.List;

public interface ProductRepositoryQuerydsl {

    List<Product> getSellingAll();

}
