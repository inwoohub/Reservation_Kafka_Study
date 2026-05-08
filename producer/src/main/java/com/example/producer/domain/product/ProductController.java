package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "1. ProductController", description = "상품 추가용 API")
@RestController
@RequestMapping("/api/product")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;

    // 생성하기
    @Operation(summary = "생성하기")
    @PostMapping
    public ResponseEntity<Void> addProduct(@RequestBody CreateProductRequest req) {
        productService.addProduct(req);
        return ResponseEntity.status(HttpStatus.CREATED).body(null);
    }

    // 전체 조회 (근데 1개만 넣을거임)
    @Operation(summary = "전체 조회")
    @GetMapping
    public ResponseEntity<List<Product>> getAllProducts() {
        return ResponseEntity.status(HttpStatus.OK).body(productService.getProducts());
    }


}
