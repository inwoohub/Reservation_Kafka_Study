# Kafka 기반 비회원 콘서트 예매 시스템

Kafka 학습을 목적으로 만든 **비회원 콘서트 예매 시스템**입니다.

회원가입 없이 이름, 생년월일, 임시 비밀번호만으로 콘서트 예매, 조회, 취소를 할 수 있습니다.

단순 CRUD 구현보다 **Kafka 이벤트 기반 처리**, **Redis 재고 차감**, **MySQL 원본 재고 반영**, **Consumer 병목 분석**을 경험하는 데 초점을 두었습니다.




---

## 프로젝트 핵심 목표

- Kafka Producer / Consumer 기반 예매 흐름 구현
- 예매 요청과 재고 처리 로직 분리
- Redis Lua Script를 이용한 빠른 재고 차감
- MySQL 원본 재고와 Redis 재고 간 정합성 보완
- k6 부하 테스트를 통한 병목 분석
- HikariCP 커넥션 풀 고갈, Querydsl UPDATE 문제 등 트러블슈팅 경험

---

## 주요 기능

- 비회원 콘서트 예매 신청
- 이름, 생년월일, 임시 비밀번호 기반 예매 조회
- 확정된 예매 취소
- Redis 기반 재고 확인 및 차감
- Kafka Topic 기반 예매 요청 / 재고 처리 결과 이벤트 발행
- MySQL 기반 예매 내역 및 원본 재고 저장

---

## 기술 스택

### Backend

- Java 21
- Spring Boot
- Spring Web
- Spring Data JPA
- Querydsl
- Spring Kafka
- MySQL
- Redis

### Infra / Test

- Docker Compose
- Apache Kafka
- Kafka KRaft Mode
- Kafka UI
- k6

---

## 최종 아키텍처

<img width="1200" height="700" alt="3차 최종" src="https://github.com/user-attachments/assets/2fdf1e12-b361-4c5f-be24-08c12193100c" />

최종 구조에서는 API 서버가 Redis Lua Script로 재고를 먼저 확인하고 차감합니다.  
Redis 차감이 성공하면 예매 내역을 저장하고 Kafka 이벤트를 발행합니다.

이후 재고 관리 서버가 이벤트를 소비해 MySQL 원본 재고를 반영하고, 처리 결과 이벤트를 다시 발행합니다.

```text
Client
  ↓
Backend 8081
  - Redis 재고 확인 및 차감
  - 예매 내역 저장
  - reservation_requested 이벤트 발행
  ↓
Kafka
  ↓
Backend 8082
  - MySQL 원본 재고 감소 / 증가
  - stock-result 이벤트 발행
  ↓
Kafka
  ↓
Backend 8081
  - 예매 상태 최종 업데이트
```

---

## 서버 역할

| 서버 | 역할 |
| --- | --- |
| Backend 8081 | 예매 요청 처리, Redis 재고 차감, 예매 내역 저장, 예매 상태 관리 |
| Backend 8082 | MySQL 원본 재고 감소 / 증가 처리 |
| Kafka Broker | 예매 요청 이벤트와 재고 처리 결과 이벤트 전달 |
| Redis | 빠른 재고 확인 및 원자적 재고 차감 |
| MySQL | 예매 내역 저장, 원본 상품 재고 저장 |

---

## Kafka Topic

| Topic | 설명 |
| --- | --- |
| `reservation_requested` | 예매 요청 이벤트 |
| `stock-result` | 원본 재고 처리 결과 이벤트 |

---

## 성능 개선 요약

초기 구조에서는 API 응답은 빠르게 끝났지만, Kafka 뒤쪽 Consumer 처리와 MySQL 작업이 밀리면서 전체 처리 시간이 길어졌습니다.

이후 다음 순서로 개선을 진행했습니다.

| 단계 | 핵심 변경 | 결과 |
| --- | --- | --- |
| 초기 구조 | Kafka 기반 비동기 처리, MySQL 조회/저장/수정 반복 | Consumer 처리 지연 발생 |
| 1차 개선 | 재고 차감을 MySQL 조건부 UPDATE로 변경 | 전체 처리 TPS 약 10.4% 증가 |
| 추가 실험 | Kafka 파티션 증가, 예약 상태 직접 UPDATE | 핵심 병목이 아니어서 큰 효과 없음 |
| 2차 개선 | Redis Lua Script로 재고 차감 | 가장 높은 처리량 기록 |
| 3차 개선 | Redis 빠른 차감 + MySQL 원본 재고 반영 | 처리량은 감소했지만 정합성 강화 |
| 트러블슈팅 | `@Async` + Virtual Thread 시도 | HikariCP 커넥션 풀 고갈 발생 |
| 최종 해결 | DB 처리 결과 확인 후 이벤트 발행 | 이벤트 순서와 재고 정합성 강화 |

자세한 성능 개선 과정은 블로그에 정리했습니다.

- [Kafka 기반 예매 시스템 성능 개선기: MySQL 병목, Redis 재고 차감, 그리고 정합성 트레이드오프](https://inscowoo.tistory.com/70)

---

## 주요 트러블슈팅

### 1. `@Async` + Virtual Thread 사용 시 HikariCP 커넥션 풀 고갈

MySQL 원본 재고 반영을 비동기로 처리하려 했지만, 실제 병목은 스레드가 아니라 DB 커넥션 풀이었습니다.

```text
HikariPool-1 - Connection is not available,
request timed out after 30003ms
(total=10, active=10, idle=0, waiting=17807)
```

`@Async`를 제거하고 MySQL 원본 재고 감소 결과를 확인한 뒤 `stock-result` 이벤트를 발행하도록 변경했습니다.

핵심적으로 배운 점은 다음과 같습니다.

```text
비동기는 병목을 제거하는 것이 아니라,
병목이 드러나는 위치를 Kafka Lag에서 Executor/HikariCP 대기열로 옮길 수 있다.
```

---

### 2. Querydsl UPDATE SET 절 순서 문제

재고가 2개이고 주문 수량이 1개인 경우, 재고는 1개로 정상 감소했지만 상품 상태가 `CLOSED`로 변경되는 문제가 있었습니다.

기대 동작은 다음과 같았습니다.

| 항목 | 값 |
| --- | --- |
| 기존 재고 | 2 |
| 주문 수량 | 1 |
| 감소 후 재고 | 1 |
| 상품 상태 | SELLING |

실제 동작은 다음과 같았습니다.

| 항목 | 값 |
| --- | --- |
| 기존 재고 | 2 |
| 주문 수량 | 1 |
| 감소 후 재고 | 1 |
| 상품 상태 | CLOSED |

원인은 Querydsl 벌크 UPDATE에서 `stock` 감소 SET이 먼저 수행되고, 이후 `status` 계산식이 변경된 `stock` 값을 다시 참조했기 때문이었습니다.

```java
// 기존
.set(product.stock, product.stock.subtract(quantity))
.set(
        product.status,
        new CaseBuilder()
                .when(product.stock.subtract(quantity).eq(0))
                .then(ProductStatus.CLOSED)
                .otherwise(product.status)
)
```

해결을 위해 상태 계산을 먼저 수행하고, 실제 재고 감소를 이후에 수행하도록 변경했습니다.

```java
// 수정
.set(
        product.status,
        new CaseBuilder()
                .when(product.stock.subtract(quantity).eq(0))
                .then(ProductStatus.CLOSED)
                .otherwise(product.status)
)
.set(product.stock, product.stock.subtract(quantity))
```

수정 후 동일 조건에서 다음 결과를 확인했습니다.

| 항목 | 값 |
| --- | --- |
| 기존 재고 | 2 |
| 주문 수량 | 1 |
| 감소 후 재고 | 1 |
| 상품 상태 | SELLING |

---

## 테스트 확인 쿼리

### 예약 상태 확인

```sql
SELECT reservation_status, COUNT(*)
FROM reservation
GROUP BY reservation_status;
```

### 상품 재고 확인

```sql
SELECT id, stock, status
FROM product;
```

### Redis 재고 확인

```bash
redis-cli GET product:stock:1
```

---

## 회고

이번 프로젝트를 통해 Kafka를 사용한다고 해서 자동으로 성능이 좋아지는 것은 아니라는 점을 배웠습니다.

초기에는 API 응답 시간이 문제라고 생각했지만, 실제 병목은 Kafka Consumer와 MySQL 처리량에 있었습니다.

Redis를 도입해 처리량을 크게 개선할 수 있었지만, 예매 시스템에서는 단순 처리량보다 재고 정합성과 이벤트 처리 순서가 더 중요했습니다.

결국 최종 구조는 가장 빠른 구조가 아니라, 충분한 성능을 유지하면서도 데이터가 틀어지지 않는 구조를 선택한 결과입니다.
