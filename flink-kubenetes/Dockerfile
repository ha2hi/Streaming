FROM flink:1.20.2-scala_2.12-java17

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.20.2

# 코드 복사
RUN mkdir /opt/flink/usrlib
COPY ./consumer.py /opt/flink/usrlib/consumer.py

ENV PYTHONPATH="/opt/flink/usrlib"

# Kafka Jar 설치
WORKDIR /opt/flink/lib
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar