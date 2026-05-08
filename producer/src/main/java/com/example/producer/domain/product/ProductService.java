package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    public void addProduct(CreateProductRequest req) {
        Product product = new Product(req);
        productRepository.save(product);
    }


    public List<Product> getProducts() {
        return productRepository.findAll();
    }
}
