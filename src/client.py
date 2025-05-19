from core import *

class Api:
    @staticmethod
    def get_tickers(is_deatils=False):
        """
        빗썸이 지원하는 KRW 마켓 코드 30개
        :param payment_currency : KRW
        :return:
        """
        resp = None
        try:
            resp = PublicApi.ticker(is_deatils)

            krw_ticker = [item["market"] for item in resp if item["market"].startswith("KRW-")]
            krw_ticker = krw_ticker[:30]
            return krw_ticker
        except Exception:
            return resp
    
    @staticmethod
    def get_price(markets):
        resp = None
        try:
            resp = PublicApi.price(markets)

            krw_price = resp[0]
            
            return krw_price
        except Exception:
            return resp