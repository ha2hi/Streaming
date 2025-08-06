import json
import time
import functools
import os
from threading import Thread
from websocket import WebSocketApp
from kafka import KafkaProducer
from kafka.errors import KafkaError
from client import Api

def retry_with_backoff(max_retries: int = 5,
                       initial_backoff: float = 1.0,
                       max_backoff: float = 30.0):
    """
    예외 발생 시 지수적 백오프를 통해 함수를 다시 시도하는 데코레이터입니다.
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
            request_timeout_ms=30000,
            linger_ms = 0,
            delivery_timeout_ms = 120000,
            enable_idempotence = True
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


class UpbitWebSocket:
    def __init__(self, request, kafka_topic, kafka_brokers, callback=print):
        self.request = request
        self.callback = callback
        self.running = False
        self.tickers = self.get_ticker()

        self.kafka_producer = KafkaDataProducerService(
            topic=kafka_topic,
            brokers=kafka_brokers
        )

        self.ws = WebSocketApp(
            url="wss://api.upbit.com/websocket/v1",
            on_message=lambda ws, msg: self.on_message(ws, msg),
            on_error=lambda ws, msg: self.on_error(ws, msg),
            on_close=lambda ws: self.on_close(ws),
            on_open=lambda ws: self.on_open(ws))


    def on_message(self, ws, msg):
        msg = json.loads(msg.decode('utf-8'))
        code = msg.get('code')
        korean_name = self.tickers.get(code)
        msg['name'] = korean_name
        
        self.kafka_producer.publish([msg])
        self.callback(msg)
    def on_error(self, ws, msg):
        self.callback(msg)
    def on_close(self, ws):
        self.callback("closed")
        self.running = False
    def on_open(self, ws):
        th = Thread(target=self.activate, daemon=True)
        th.start()
    def activate(self):
        self.ws.send(self.request)
        while self.running:
            time.sleep(1)
        self.ws.close()
    def start(self):
        self.running = True
        self.ws.run_forever()

    @staticmethod
    def get_ticker():
        upbit_tickers = Api.get_upbit_tickers()
        bithumb_tickers = Api.get_tickers()

        tickers = [u_ticker for u_ticker in upbit_tickers if u_ticker[0] in map(lambda x: x[0], bithumb_tickers)]

        return dict(tickers)
