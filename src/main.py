from __future__ import annotations

from kafka import KafkaProducer
from json import dumps
import json
from kafka.errors import KafkaError
import functools
import time
import os

BROKER_LIST = ["15.164.218.105:9091", "15.164.218.105:9092", "15.164.218.105:9093"]
TOPIC = "transction"

def retry_with_backoff(max_retries: int = 5,
                       initial_backoff: float = 1.0,
                       max_backoff: float = 30.0):
    """
    예외 발생 시 지수적 백오프를 사용하여 함수를 다시 시도하는 데코레이터입니다.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            backoff = initial_backoff
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries > max_retries:
                        print(f"{func.__name__} failed after {retries} attempts: {e}")
                        raise
                    print(f"{func.__name__} error: {e}, retry {retries}/{max_retries} in {backoff}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        return wrapper
    return decorator
        
class KafkaDataProducerService:
    """
    Description:
        API Data to Kafka
    Params:
        - topic_name (str, Required) : Topic명
        - brokers (List[str], Required) : Broker 목록
        - retries (Optional[int]) : 재시도 횟수
    """
    def __init__(self, topic, brokers, retries = 3):
        self.topic = topic
        self.brokers = brokers
        self.retries = retries
        
        self.producer = self._create_producer()

    @retry_with_backoff(max_retries = int(os.getenv("RETRY_MAX", 6)), 
                        initial_backoff = float(os.getenv("RETRY_INITIAL_BACKOFF", 1.0)), 
                        max_backoff = float(os.getenv("RETRY_MAX_BACKOFF", 30.0)))
    def _create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers = self.brokers,
            retries = self.retries,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000
        )

    def publish(self, messages):
        for messgae in messages:
            try:
                future = self.producer.send(self.topic, messgae)
                metadata = future.get(timeout=10)
                print(f"Publish to {self.topic} partition = {metadata.partition} offset = {metadata.offset}")
            except KafkaError as e:
                print(f"Kafka publish error : {e}")
        try:
            self.producer.flush()
        except KafkaError as e:
            print(f"Error flushing producer: {e}")

    def close(self):
        try:
            self.producer.close()
            print("Kafka producer closed successfully.")
        except Exception as e:
            print(f"Error closing producer: {e}")

if __name__ == "__main__":
    from client import Api
    
    service = KafkaDataProducerService(TOPIC, BROKER_LIST)
    try:
        tickers = Api.get_tickers()
        if not tickers:
            print("No tickers retrived; aborting")
        else:
            messages = [Api.get_price(t) for t in tickers]
            service.publish(messages)
    except Exception as e:
        print(f"Unexpected error in data publishing workflow: {e}")
    finally:
        service.close()