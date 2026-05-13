/**
 * k6 부하 테스트 스크립트입니다.
 * 
 * 대상 : 예매하기 API
 * 
 * 방법 : 랜덤하게 값을 채워넣어서 초당 100개씩 30초 동안
 * 
 * 실행 방법 : 
 * 1. 설치
 * brew install k6 
 * 
 * 2. 실행
 * k6 run k6-reservation-test.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';



const startTime = new Date();

console.log(`===== k6 테스트 시작 =====`);

console.log(`시작 시간: ${startTime.toString()}`);

console.log(`테스트 대상: POST http://localhost:8081/api/reservation`);

export const options = {
  vus: 1000,
  duration: '100s',
};

export default function () {
  const randomNum = Math.floor(Math.random() * 100000000);

  const payload = JSON.stringify({
    productId: 1,
    quantity: 1,
    buyerName: `buyer-${randomNum}`,
    birthDate: `19990101`,
    teamPassword: `${1000 + Math.floor(Math.random() * 9000)}`
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post('http://localhost:8081/api/reservation', payload, params);

  check(res, {
    'status is 201': (r) => r.status === 201,
    'response time < 500ms': (r) => r.timings.duration < 500,
  });

  sleep(1);
}