- 권한 설정
```
kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
```

- RUN Application Mode
```
./bin/flink run-application \
  --target kubernetes-application \
  --parallelism 3 \
  -Dkubernetes.cluster-id=my-application \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dkubernetes.container.image=hiha2/pyflink-test:1.20.2 \
  -Dkubernetes.rest-service.exposed.type=NodePort \
  -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.2.jar \
  -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.2.jar \
  --pyModule consumer \
  --pyFiles /opt/flink/usrlib/consumer.py
  ```