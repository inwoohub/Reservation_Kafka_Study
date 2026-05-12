package com.example.producer.domain.product.querydsl;

import com.example.producer.domain.product.Product;
import com.example.producer.domain.product.ProductStatus;
import com.example.producer.domain.product.QProduct;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static com.example.producer.domain.product.QProduct.*;

@RequiredArgsConstructor
public class ProductRepositoryImpl implements ProductRepositoryQuerydsl {

    private final JPAQueryFactory jpaQueryFactory;

    @Override
    public List<Product> getSellingAll() {
        return jpaQueryFactory
                .select(product)
                .from(product)
                .where(product.status.eq(ProductStatus.SELLING))
                .fetch();
    }

}
