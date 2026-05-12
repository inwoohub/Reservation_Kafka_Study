package com.example.stockconsumer.global.error;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class ApiException extends RuntimeException {

    private final HttpStatus httpStatus;

    private final String status;

    private final String code;

    private final String message;


    /**
     * 디버깅 용이 아니라면, 이걸로 사용
     */
    public ApiException(HttpStatus httpStatus, String status, String code, String message) {
        super(message);
        this.httpStatus = httpStatus;
        this.status = status;
        this.code = code;
        this.message = message;
    }

    /**
     * Throwsable -> 진짜 예외가 터진 이유를 찾기 위함
     * 디버깅 용도로 추천
     */
    public ApiException(Throwable cause, HttpStatus httpStatus, String status, String code, String message) {
        super(cause);
        this.httpStatus = httpStatus;
        this.status = status;
        this.code = code;
        this.message = message;
    }

}