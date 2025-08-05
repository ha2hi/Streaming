# Streaming
빗썸 API의 데이터를 활용하여 실시간 거래량을 확인하기 위한 프로젝트입니다.  
  
Apache Flink와 Kafka를 기반으로 한 스트리밍 데이터 처리 애플리케이션입니다.  
Kafka는 Docker Compose를 활용하여 구성했고, Flink 또한 로컬에서 실행할 수 있도록 구성하였습니다.  

## 아키텍처
<img width="1000" height="454" alt="Image" src="https://github.com/user-attachments/assets/a42c84d4-9239-42ca-964d-88d3b5f42514" />  

# 환경 구성
## Docker
### Docker 설치
```
sudo apt-get update

# Docker 필요 패키지 설치
sudo apt-get install apt-transport-https ca-certificates curl gnupg-agent software-properties-common

# GPG 키 추가
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

# 공식 apt 저장소 추가
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

sudo apt-get update

# docker 설치
sudo apt-get install docker-ce docker-ce-cli containerd.io

```  

### docker-compose 설치
```
# docker-compose 설치
# 버전 확인 : https://github.com/docker/compose/releases
sudo curl -L "https://github.com/docker/compose/releases/download/v2.5.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# 권한 부여
sudo chmod +x /usr/local/bin/docker-compose

# 심볼릭 링크 연결
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# 버전 확인
docker-compose --version
```  

### Docker Login
```
docker login
```

## AWS EKS
### AWS CLI v2 설치
```
# uzip 설치
apt install unzip

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# 설치 확인
aws --version

# AWS 연결
aws configure
```  

### eksctl 설치
```
curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp
sudo mv /tmp/eksctl /usr/local/bin

eksctl version
```  

### kubectl 설치
```
curl -o kubectl https://amazon-eks.s3.us-west-2.amazonaws.com/1.19.6/2021-01-05/bin/linux/amd64/kubectl
chmod +x ./kubectl
mkdir -p $HOME/bin && cp ./kubectl $HOME/bin/kubectl && export PATH=$PATH:$HOME/bin
echo 'export PATH=$PATH:$HOME/bin' >> ~/.bashrc

kubectl version --short --client
```

### EKS Cluster 생성
```
# 환경 변수
export K8S_VERSION="1.31"
export AWS_DEFAULT_REGION="ap-northeast-2"
export CLUSTER_NAME="flink-application-cluster"

# EKS 클러스터 생성
envsubst < eks-cluster.yaml | eksctl create cluster -f -

# kubeconfig 파일 생성
aws eks update-kubeconfig --region <REGION_CODE> --name <CLUSTER_NAME>
```  

## Apache Flink 설치
### JDK 17 설치
```
sudo apt-get update 

sudo apt-get install openjdk-17-jdk

# Java 버전 확인
java -version

# Java 설치 경로 확인
readlink -f $(which java)

# JAVA_HOME 경로 등록
vi /etc/profile​
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin

# 환경변수 적용
source /etc/profile

# 확인
echo $JAVA_HOME
echo $PATH | grep java
```  

### Flink 1.20.2 설치
```
wget https://dlcdn.apache.org/flink/flink-1.20.2/flink-1.20.2-bin-scala_2.12.tgz

# 압축 해제
tar -xvzf flink-1.20.2-bin-scala_2.12.tgz
```

## 파이썬 가상환경
### Conda 설치
```
sudo apt update

sudo apt install curl -y

curl --output anaconda.sh https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh
sha256sum anaconda.sh
bash anaconda.sh

# 환경 변수 추가
sudo vi ~/.bashrc
export PATH=~/anaconda3/bin:~/anaconda3/condabin:$PATH
source ~/.bashrc

conda -V
```

### 가상환경 생성
```
conda create -n streaming_python python=3.9 -y
source ~/anaconda3/etc/profile.d/conda.sh

conda activate streaming_python

pip install -r requirements.txt
```

# 실행 항법
1. 레포지토리 클론
```
# git 설치
sudo apt-get install git -y

# git 버전 확인
git --version

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
- Docker Custom 이미지 생성
```
cd flink-kubernetes

# docker build
docker build -t hiha2/pyflink:1.20.2 .

# docker push
docker push hiha2/pyflink:1.20.2
```
- 권한 추가
```
kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
```  

- Application Mode 실행
```
./bin/flink run-application \
  --target kubernetes-application \
  -Dkubernetes.cluster-id=my-application \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dkubernetes.container.image=hiha2/test_job10 \
  -Dkubernetes.rest-service.exposed.type=NodePort \
  -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.2.jar \
  -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.2.jar \
  -Drestart-strategy.type=exponential-delay \
  -Drestart-strategy.exponential-delay.initial-backoff=1s \
  -Drestart-strategy.exponential-delay.backoff-multiplier=2 \
  -Drestart-strategy.exponential-delay.max-backoff=10s \
  --pyModule consumer \
  --pyFiles /opt/flink/usrlib/consumer.py
```

5. Stremlit 실행
```
cd view

docker build -t hiha2/view:0.0.1 .

docker push hiha2/view:0.0.1

kubectl apply -f deployment.yml
kubectl apply -f service.yml
```  

6. 결과
<img width="1500" height="438" alt="Image" src="https://github.com/user-attachments/assets/21d9772b-451b-47b9-82d3-6bec2d685f49" />

# 제한 사항
[Bithumb API]  
- 빗썸 API는 1초당 150회 요청 가능합니다.  
- 초과 요청을 하시는 경우 API 사용이 일시적으로 제한됩니다.
  
# Task lists
- [x] Standalone to Kubernetes
- [x] Using RocksDB State Backend
- [x] Failure Recover
- [ ] UPbit trading volume added
- [ ] Use Websocket
- [ ] Using Karpenter
