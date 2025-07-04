from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# 1. 환경 설정
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(3)
env.add_jars("file:///Users/mzc01-hyucksangcho/Downloads/flink-sql-connector-kafka-3.3.0-1.19.jar")

settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)
t_env.get_config().set_local_timezone("Asia/Seoul")

# 2. Kafka 소스 테이블 생성

t_env.execute_sql("""
CREATE TABLE transaction (
    market STRING,
    trade_date STRING,
    trade_time STRING,
    trade_date_kst STRING,
    trade_time_kst STRING,
    trade_timestamp BIGINT,
    opening_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    trade_price DOUBLE,
    prev_closing_price DOUBLE,
    change STRING,
    change_price DOUBLE,
    change_rate DOUBLE,
    signed_change_price DOUBLE,
    signed_change_rate DOUBLE,
    trade_volume DOUBLE,
    acc_trade_price DOUBLE,
    acc_trade_price_24h DOUBLE,
    acc_trade_volume DOUBLE,
    acc_trade_volume_24h DOUBLE,
    highest_52_week_price DOUBLE,
    highest_52_week_date STRING,
    lowest_52_week_price DOUBLE,
    lowest_52_week_date STRING,
    `timestamp` BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'transction',
    'properties.bootstrap.servers' = '54.180.149.232:9091',
    'properties.group.id' = 'flink-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
""")

# 3. 거래 금액 집계 뷰 생성 (1분 윈도우)

t_env.execute_sql(
"""
CREATE TEMPORARY VIEW trade_volume_per_minute AS(
SELECT
    market,
    window_start, 
    window_end,
    FLOOR(SUM(trade_price * trade_volume)) as one_m_trade_volume
FROM TABLE(
    TUMBLE(TABLE transaction, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)
)
GROUP BY market, window_start, window_end
)
""")

# 4. 윈도우별 Top 5 추출

t_env.execute_sql("""
CREATE TEMPORARY VIEW top5_trade_volume_per_minute AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end 
            ORDER BY one_m_trade_volume DESC
        ) AS row_num
    FROM trade_volume_per_minute
)
WHERE row_num <= 5
""")

t_env.execute_sql("SELECT * FROM top5_trade_volume_per_minute").print()

