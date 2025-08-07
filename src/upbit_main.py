import json
from upbit_client import UpbitWebSocket
from client import Api

if __name__ == "__main__":
    # 실행 시 입력할 Kafka broker 주소와 topic 이름
    BROKER_LIST = ["13.125.92.214:9091"]
    TOPIC = "upbit"

    # 업비트&빗썸 공통 티커
    bithumb_tickers = Api.get_tickers()
    upbit_tickers = Api.get_upbit_tickers()

    tickers = dict([u_ticker for u_ticker in upbit_tickers if u_ticker[0] in map(lambda x : x[0], bithumb_tickers)])

    # 구독 요청 구성
    request_data = json.dumps([
        {"ticket": "test"},
        {"type": "ticker", "codes": list(tickers.keys())}
    ])

    # WebSocket 시작
    ws = UpbitWebSocket(
        request=request_data,
        kafka_topic=TOPIC,
        kafka_brokers=BROKER_LIST
    )
    ws.start()