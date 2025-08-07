from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Kafka 파티션 수
    env.set_parallelism(3)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().set_local_timezone("Asia/Seoul")

    # set the checkpoint mode to EXACTLY_ONCE
    t_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    t_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "1 min")

    # set the statebackend type to "rocksdb", other available options are "hashmap"
    t_env.get_config().set("state.backend.type", "rocksdb")

    # set the checkpoint directory, which is required by the RocksDB statebackend
    # 작업 종료 시에도 CheckPoint를 삭제되지 않기 위해 "RETAIN_ON_CANCELLATION" 사용
    t_env.get_config().set("execution.checkpointing.dir", "s3a://pyflink-test-hs/upbit/checkpoint/")
    t_env.get_config().set("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION")


    # 2. Kafka 소스 테이블 생성
    t_env.execute_sql("""
    CREATE TABLE upbit_transaction (
        type STRING,
        code STRING,
        opening_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        trade_price DOUBLE,
        prev_closing_price DOUBLE,
        acc_trade_price DOUBLE,
        change STRING,
        change_price DOUBLE,
        signed_change_price DOUBLE,
        change_rate DOUBLE,
        signed_change_rate DOUBLE,
        ask_bid STRING,
        trade_volume DOUBLE,
        acc_trade_volume DOUBLE,
        trade_date STRING,
        trade_time STRING,
        trade_timestamp BIGINT,
        acc_ask_volume DOUBLE,
        acc_bid_volume DOUBLE,
        highest_52_week_price DOUBLE,
        highest_52_week_date STRING,
        lowest_52_week_price DOUBLE,
        lowest_52_week_date STRING,
        market_state STRING,
        is_trading_suspended BOOLEAN,
        delisting_date STRING,
        market_warning STRING,
        `timestamp` BIGINT,
        acc_trade_price_24h DOUBLE,
        acc_trade_volume_24h DOUBLE,
        stream_type STRING,
        name STRING,
        event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'upbit',
        'properties.bootstrap.servers' = '13.125.92.214:9091',
        'properties.group.id' = 'flink-upbit-group',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """)

    # 3. 거래 금액 집계 뷰 생성 (1분 윈도우)
    t_env.execute_sql("""
    CREATE TEMPORARY VIEW trade_volume_per_minute_upbit AS 
    SELECT
        code AS market,
        name,
        window_start, 
        window_end,
        FLOOR(SUM(trade_price * trade_volume)) as one_m_trade_volume
    FROM TABLE(TUMBLE(TABLE upbit_transaction, DESCRIPTOR(event_time), INTERVAL '1' MINUTES))
    GROUP BY code, name, window_start, window_end
    """)

    #table_result1 = t_env.execute_sql("SELECT * FROM trade_volume_per_minute")
    #table_result1.print()

    # 4. 윈도우별 Top 5 추출

    t_env.execute_sql("""
    CREATE TEMPORARY VIEW top5_trade_volume_per_minute_upbit AS
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY window_start, window_end 
                ORDER BY one_m_trade_volume DESC
            ) AS row_num
        FROM trade_volume_per_minute_upbit
    )
    WHERE row_num <= 5
    """)

    # 5. S3 Sink 저장
    # Partition으로 컬럼을 사용하면 데이터에는 해당 컬럼이 저장안됨.
    t_env.execute_sql(f"""
    CREATE TABLE s3_sink (
        name STRING,
        market STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        one_m_trade_volume DOUBLE,
        row_num BIGINT,
        `year` STRING,
        `month` STRING,
        `day` STRING
    ) PARTITIONED BY (`year`, `month`, `day`)
    WITH (
        'connector' = 'filesystem',
        'path' = 's3a://pyflink-test-hs/upbit/trade_volume_per_minute/',
        'sink.partition-commit.policy.kind'='success-file',
        'format' = 'json'
    )
    """)

    table_result = t_env.execute_sql("""
    INSERT INTO s3_sink 
    SELECT
        name,  
        market,
        window_start,
        window_end,
        one_m_trade_volume,
        row_num,
        DATE_FORMAT(window_start, 'yyyy') as `year`,
        DATE_FORMAT(window_start, 'MM') as `month`,
        DATE_FORMAT(window_start, 'dd') as `day`
    FROM top5_trade_volume_per_minute_upbit;
    """)

    table_result.wait()
    
if __name__ == "__main__":
    main()
