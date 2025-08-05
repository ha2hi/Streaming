from __future__ import annotations

from requests.adapters import HTTPAdapter
import requests
from urllib3.util.retry import Retry

__all__ = ["PublicApi"]

class PublicApi:
    @staticmethod
    def ticker(is_deatils = False):
        """
        Description:
            마켓 코드 조회
            https://apidocs.bithumb.com/reference/%EB%A7%88%EC%BC%93%EC%BD%94%EB%93%9C-%EC%A1%B0%ED%9A%8C
        Params:
            is_deatils (boolean, Defaults to false) : 유의종목 필드과 같은 상세 정보 노출 여부
        Return:
            list : [{'market': 'KRW-BTC', 'korean_name': '비트코인', 'english_name': 'Bitcoin'}]
        """
        uri = f"/v1/market/all?isDetails={is_deatils}"
        return BithumbHttp().get(uri)
    
    @staticmethod
    def price(markets: str):
        """
        Description:
            시세 현재가 조회
            https://apidocs.bithumb.com/reference/%ED%98%84%EC%9E%AC%EA%B0%80-%EC%A0%95%EB%B3%B4
        Params:
            markets (Str, Required) : 반점으로 구분되는 마켓 코드 (ex. KRW-BTC, BTC-ETH)
        Return:
            dict : [{'market': 'KRW-BTC', 'trade_date': '20250510', 'trade_time': '051423', 'trade_date_kst': '20250510', 'trade_time_kst': '141423', 'trade_timestamp': 1746886463399, 'opening_price': 144520000, 'high_price': 144678000, 'low_price': 143702000, 'trade_price': 144210000, 'prev_closing_price': 144520000, 'change': 'FALL', 'change_price': 310000, 'change_rate': 0.0021, 'signed_change_price': -310000, 'signed_change_rate': -0.0021, 'trade_volume': 4.204e-05, 'acc_trade_price': 28611053788.18734, 'acc_trade_price_24h': 100828660660.68262, 'acc_trade_volume': 198.55651572, 'acc_trade_volume_24h': 697.72846838, 'highest_52_week_price': 163460000, 'highest_52_week_date': '2025-01-21', 'lowest_52_week_price': 71573000, 'lowest_52_week_date': '2024-08-06', 'timestamp': 1746854065313}]
        """
        uri = f"/v1/ticker?markets={markets}"
        return BithumbHttp().get(uri)
    
    @staticmethod
    def upbit_ticker(is_deatils = False):
        """
        Description:
            마켓 코드 조회
            https://docs.upbit.com/kr/reference/%EB%A7%88%EC%BC%93-%EC%BD%94%EB%93%9C-%EC%A1%B0%ED%9A%8C
        Params:
            is_deatils (boolean, Defaults to false) : 유의종목 필드과 같은 상세 정보 노출 여부
        Return:
            list : [{'market': 'KRW-BTC', 'korean_name': '비트코인', 'english_name': 'Bitcoin'}]
        """
        uri = f"/v1/market/all?is_details={is_deatils}"
        return UpbitHttp().get(uri)


class HttpMethod:
    def __init__(self):
        self.session = self._requests_retry_session()

    def _requests_retry_session(self, retries=5, backoff_factor=0.3,
                                status_forcelist=(500, 502, 504), session=None):
        
        with requests.Session() as s:
        
            retry = Retry(total=retries, read=retries, connect=retries,
                            backoff_factor=backoff_factor,
                            status_forcelist=status_forcelist)
            adapter = HTTPAdapter(max_retries=retry)
            s.mount('http://', adapter)
            s.mount('https://', adapter)
            return s
    
    def get(self, path, timeout=3, **kwargs):
        try:
            uri = self.base_url + path
            return self.session.get(url=uri, params=kwargs, timeout=timeout).\
                json()
        except Exception as x:
            print("It failed", x.__class__.__name__)
            return None

class BithumbHttp(HttpMethod):
    def __init__(self, conkey="", seckey=""):
        self.API_CONKEY = conkey.encode('utf-8')
        self.API_SECRET = seckey.encode('utf-8')
        super(BithumbHttp, self).__init__()

    @property
    def base_url(self):
        return "https://api.bithumb.com"
    
class UpbitHttp(HttpMethod):
    def __init__(self, conkey="", seckey=""):
        self.API_CONKEY = conkey.encode('utf-8')
        self.API_SECRET = seckey.encode('utf-8')
        super(UpbitHttp, self).__init__()
    @property
    def base_url(self):
        return "https://api.upbit.com"