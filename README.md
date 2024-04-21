# DBT

## Setting up Spark:
### We use bitnami's prebuilt spark version. We can access the interactive shell by:
```bash
docker run --name spark -it bitnami/spark:latest /bin/bash
```
### This will create a Docker container of the name spark and pull the latest bitnami spark build.

### Navigate to your desired folder, clone the directory and then use the following command to test batch processing:

```bash
spark-submit 1.py
```


Stream Processing:
1. Start kafka and zookeeper by navigating to the kafka directory
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
Start spark:
cd /opt/spark
./sbin/start-all.sh
2.   Start 3 consumers in 3 different terminals
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 [consb.py](http://consb.py/)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 [consc.py](http://consc.py/)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 [consh.py](http://consh.py/)
3. Start producer in another terminal
python3 stream_producer.py
