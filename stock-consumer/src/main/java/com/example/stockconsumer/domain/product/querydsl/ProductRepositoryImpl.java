package com.example.stockconsumer.domain.product.querydsl;


import com.example.stockconsumer.domain.product.ProductStatus;
import com.example.stockconsumer.domain.product.QProduct;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;

import static com.example.stockconsumer.domain.product.QProduct.*;

@RequiredArgsConstructor
public class ProductRepositoryImpl implements ProductRepositoryQuerydsl {

    private final JPAQueryFactory jpaQueryFactory;

    /**
     * 애플리케이션의 영속성 컨텍스트에서 처리를 하지않고,
     * DB에서 처리 후 애플리케이션은 분기 처리만 하며 성능을 올려보기 Try
     *
     * @param productId : 제품 id
     * @param quantity : 주문 수량
     * @param selling : 제품 판매 중
     * @param closed : 제품 판매 종료
     * @return boolean (성공 시 true, 실패 시 false)
     */
    @Override
    public boolean decreaseStock(Long productId, Integer quantity, ProductStatus selling, ProductStatus closed) {

        long updateCount = jpaQueryFactory
                .update(product)
                .set(product.stock, product.stock.subtract(quantity))
                .set(
                        product.status, new CaseBuilder()
                                .when(product.stock.subtract(quantity).eq(0))
                                .then(ProductStatus.CLOSED)
                                .otherwise((product.status))
                )
                .where(
                        product.id.eq(productId),
                        product.status.eq(selling),
                        product.stock.goe(quantity)
                )
                .execute(); // execute 에 반환값은 영향받은 row 수임.

        return updateCount == 1L;
    }

}
