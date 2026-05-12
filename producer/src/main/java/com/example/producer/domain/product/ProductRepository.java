package com.example.producer.domain.product;

import com.example.producer.domain.product.querydsl.ProductRepositoryQuerydsl;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long>, ProductRepositoryQuerydsl {



}
