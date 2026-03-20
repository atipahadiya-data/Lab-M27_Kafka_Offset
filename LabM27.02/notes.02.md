# setup project directory

mkdir LabM27.02
cd LabM27.02

touch replay_tool.py
touch offset_reset_tool.py
touch idempotent_consumer.py
touch lag_dashboard.py
touch requirements.txt

# requirements.txt
confluent-kafka==2.3.0

# setup environment 
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
pip install -r requirements.txt

# Start Kafka with Docker Desktop
Create docker-compose.yml

# Start Kafka
docker compose up -d

verify : docker compose ps

# to stop kafka use 
docker compose down

# create topics 
docker exec -it labm2702-kafka-1 bash

kafka-topics --create \
  --topic streaming.user.interactions \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1

  # Produce Test Data
  create simple producer 
  run : python3 producer.py

# run all lab compoenets : 

# Replay from beginning (first 50 events)
python3 replay_tool.py --topic streaming.user.interactions \
  --mode beginning --limit 50
 
# Replay from 1 hour ago
python3 replay_tool.py --topic streaming.user.interactions \
  --mode time --hours-ago 1 --limit 100
 
# Replay from specific offset
python3 replay_tool.py --topic streaming.user.interactions \
  --mode offset --partition 0 --offset 500 --limit 20
 
# Save replay to file
python3 replay_tool.py --topic streaming.user.interactions \
  --mode time --hours-ago 0.5 --limit 1000 \
  --output replay_output.json

# Test the offset reset tool:

# Check current status
python3 offset_reset_tool.py --group streampulse-analytics-v1 \
  --topic streaming.user.interactions --action status

# Reset to beginning (reprocess everything)
python3 offset_reset_tool.py --group streampulse-analytics-v1 \
  --topic streaming.user.interactions --action earliest

# Reset to 2 hours ago
python3 offset_reset_tool.py --group streampulse-analytics-v1 \
  --topic streaming.user.interactions --action timestamp --hours-ago 2

# Skip to latest (ignore backlog)
python3 offset_reset_tool.py --group streampulse-analytics-v1 \
  --topic streaming.user.interactions --action latest


# Part 3: Idempotent Consumer
python3 idempotent_consumer.py (Let it process ~1000+ events)

Now simulate replay
Stop consumer (Ctrl+C)
Reset offsets:
python3 offset_reset_tool.py \
  --group idempotent-consumer-v1 \
  --topic streaming.user.interactions \
  --action timestamp \
  --hours-ago 1

restart consumer : python3 idempotent_consumer.py

Output : Duplicates skipped: XXX

# Part 4: Lag Dashboard

python3 lag_dashboard.py
