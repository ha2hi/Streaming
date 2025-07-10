import boto3
from datetime import date
import pandas as pd
from io import StringIO
import streamlit as st

# 오늘 날짜 구성
today = date.today()
year = today.year
month = today.month
day = today.day

# S3 클라이언트 초기화
s3 = boto3.client('s3')

bucket_name = 'pyflink-test-hs'
prefix = f'trade_volume_per_minute/year={year}/month={month:02d}/day={day:02d}/'

@st.cache_data
def load_data(content):
    # df = pd.read_json(content, lines=True)
    df = pd.read_json(StringIO(content), lines=True)

    return df
    
try:
    # 객체 목록 가져오기
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # 객체가 존재하는 경우
    if 'Contents' in response:
        # _SUCCESS 제외 후 가장 최근에 생성된 객체 찾기
        files = [obj for obj in response['Contents'] if not obj['Key'].endswith('_SUCCESS')]
        
        if not files:
            print("파일이 없습니다.")
        else:
            latest_file = max(files, key=lambda x: x['LastModified'])
            latest_key = latest_file['Key']
            # print("가장 최근 파일:", latest_key)

            # 예: 파일 내용 읽기 (텍스트 파일이라고 가정)
            obj = s3.get_object(Bucket=bucket_name, Key=latest_key)
            content = obj['Body'].read().decode('utf-8')
            
            df = load_data(content)
            
            st.title("📊 Trade Volume Per Minute")
            # st.write("Flink에서 저장된 데이터를 Streamlit에서 시각화합니다.")
            
            st.dataframe(df)
    else:
        print("해당 prefix에 객체가 없습니다.")

except Exception as e:
    print("에러 발생:", e)