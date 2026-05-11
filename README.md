# Kafka 기반 비회원 콘서트 예매 시스템

Kafka 학습을 목적으로 만든 **비회원 콘서트 예매 시스템**입니다.

사용자는 회원가입 없이 이름, 생년월일, 임시 비밀번호만으로 콘서트 예매를 요청하고, 예매 내역을 조회하거나 취소할 수 있습니다.

이 프로젝트는 단순 CRUD보다 **Kafka를 이용한 이벤트 기반 처리 흐름**을 이해하는 데 초점을 둡니다.

---

## 프로젝트 목적

이 프로젝트의 핵심 목적은 Kafka Producer와 Consumer를 이용해 예매 요청을 비동기 이벤트로 처리하는 구조를 학습하는 것입니다.

예매 요청이 들어오면 API 서버는 바로 예매를 확정하지 않고 Kafka Topic에 이벤트를 발행합니다.  
이후 여러 Consumer 서버가 이벤트를 구독하여 예약 내역 저장, 재고 확인, 예매 확정 또는 실패 처리를 수행합니다.

---

## 주요 기능

- 비회원 콘서트 예매 요청
- 이름, 생년월일, 임시 비밀번호 기반 예매 내역 조회
- 확정된 예매 취소
- Kafka Topic을 통한 예약 이벤트 발행 및 구독
- Consumer를 통한 예약 내역 저장
- Consumer를 통한 재고 확인 및 상태 변경

---

## 기술 스택

### Backend

- Java 21
- Spring Boot
- Spring Web
- Spring Data JPA
- Spring Kafka
- MySQL
- Lombok

### Frontend

- Android
- Jetpack Compose

### Infra

- Docker Compose
- Apache Kafka
- Kafka KRaft Mode
- Kafka UI
- MySQL 8.0

---

## 전체 구조

```text
[Android App]
     ↓
[Reservation API Server]
     ↓ Kafka Event 발행
[Kafka Topic]
     ↓
[Reservation History Consumer]
     ↓
[MySQL]

[Kafka Topic]
     ↓
[Stock Consumer]
     ↓
[MySQL]
