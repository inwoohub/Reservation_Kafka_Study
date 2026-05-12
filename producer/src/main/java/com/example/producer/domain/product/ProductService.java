package com.example.producer.domain.product;

import com.example.producer.domain.product.dto.CreateProductRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    @Transactional
    public void addProduct(CreateProductRequest req) {
        Product product = new Product(req);
        productRepository.save(product);
    }


    public List<Product> getProducts() {
        return productRepository.findAll();
    }

}
