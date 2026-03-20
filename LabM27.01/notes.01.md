step 1 : Create a docker-compose.yml with 3 Kafka brokers and 1 ZooKeeper. Configure:

KAFKA_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_MIN_INSYNC_REPLICAS: 2
KAFKA_NUM_PARTITIONS: 6

- Verify all 3 brokers are online:
    docker compose up -d

    docker compose ps

Step 1 : Task B: Create a Fault-Tolerant Topic

docker exec -it kafka-1 ls /usr/bin/

# create kafka topic :
docker exec -it kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic ft-streampulse-events \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

# verify the topic : 
docker exec -it kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic ft-streampulse-events

# Summary
✔ Use kafka-topics (not .sh)
✔ Run via docker exec
✔ Use kafka1:9092 inside container

# Document the topic layout :
```
| Partition | Leader | Replicas | ISR     |
|----------|--------|----------|---------|
| 0        | 3      | 3,2,1    | 3,2,1   |
| 1        | 1      | 1,3,2    | 1,3,2   |
| 2        | 2      | 2,1,3    | 2,1,3   |
| 3        | 3      | 3,1,2    | 3,1,2   |
| 4        | 1      | 1,2,3    | 1,2,3   |
| 5        | 2      | 2,3,1    | 2,3,1   |

```

# Part 3: Failure Simulation Scenarios
- Run producer and consumer , then execute each failure scenario during operation.

python3 ft_producer.py

python3 ft_consumer.py

python3 kafka_failure_simulation.py

# Scenario 1: Single Follower Crash
-  While producer and consumer are running:

    docker stop kafka3

-  Wait 30 seconds, observe producer/consumer output

-  Bring back
docker start kafka3

itentify leader :
docker exec kafka1 kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic ft-streampulse-events | head -3





  | Scenario           | Producer Errors | Consumer Errors | Events Lost | Duplicates | Recovery Time | Commands |
|-------------------|----------------|----------------|------------|-----------|---------------|----------|
| 1. Follower crash  | ?              | ?              | ?          | ?         | ?             | Stop follower broker (`docker stop kafka2`) |
| 2. Leader crash    | ?              | ?              | ?          | ?         | ?             | Stop leader broker (`docker stop kafka1`) |
| 3. Two brokers down|             | ?              | ?          | ?         | ?             | Stop two brokers (`docker stop kafka2 kafka3`) |
| 4. Consumer hard kill | ?           | ?              | ?          | ?         | ?             | Kill consumer process (`kill -9 <pid>`) |
| 5. Rolling restart | ?              | ?              | ?          | ?         | ?             | Restart brokers one by one (`docker restart kafka1 kafka2 kafka3`) |


