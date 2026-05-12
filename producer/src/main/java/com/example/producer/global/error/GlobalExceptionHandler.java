package com.example.producer.global.error;

import com.example.producer.global.error.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApiException(ApiException e, HttpServletRequest req) {

        ErrorResponse errorResponse = new ErrorResponse(e.getStatus(), e.getCode(), e.getMessage());

        return ResponseEntity.status(e.getHttpStatus()).body(errorResponse);

    }

}
