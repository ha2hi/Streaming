# Streaming
---
빗썸 API의 데이터를 활용하여 실시간 거래량을 확인하기 위한 프로젝트입니다.  
  
Apache Flink와 Kafka를 기반으로 한 스트리밍 데이터 처리 애플리케이션입니다.  
Kafka는 Docker Compose를 활용하여 구성했고, Flink 또한 로컬에서 실행할 수 있도록 구성하였습니다.  
  
### 실행 항법
1. 레포지토리 클론
```
git clone https://github.com/ha2hi/Streaming.git
cd Streaming
```  
2. Kafka 환경 구성
```
- 외부 IP 입력
export DOCKER_HOST_IP=<YOUR_PUBLIC_IP>

docker-compose up -d
```  
3. API 데이터 수집
```
python src/main.py
```  
4. Flink 애플리케이션 실행
```
python src/consumer.py
```
  
### 제한 사항
- 빗썸 API는 1초당 150회 요청 가능합니다.  
- 초과 요청을 하시는 경우 API 사용이 일시적으로 제한됩니다.