# replay_tool.py
"""
StreamPulse Event Replay Tool
Re-read events from a specific time, offset, or from the beginning.
"""
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END
import json
import time
import argparse
from datetime import datetime, timedelta

class ReplayTool:
    def __init__(self, bootstrap_servers, group_id):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

    def replay_from_beginning(self, topic, limit=None):
        """Replay all events from the beginning of a topic."""
        consumer = Consumer(self.config)

        def seek_beginning(c, partitions):
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            c.assign(partitions)

        consumer.subscribe([topic], on_assign=seek_beginning)

        count = 0
        events = []

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    if count > 0:
                        break  # No more events
                    continue
                if msg.error():
                    continue

                event = json.loads(msg.value().decode('utf-8'))
                events.append({
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'event': event,
                })
                count += 1

                if limit and count >= limit:
                    break
        finally:
            consumer.close()

        return events

    def replay_from_timestamp(self, topic, hours_ago, limit=None):
        """Replay events from a specific time (hours ago)."""
        consumer = Consumer(self.config)
        consumer.subscribe([topic])

        # Wait for partition assignment
        consumer.poll(5.0)
        assignment = consumer.assignment()

        if not assignment:
            print('No partitions assigned')
            consumer.close()
            return []

        # Seek to timestamp
        target_ms = int((time.time() - hours_ago * 3600) * 1000)
        tps = [TopicPartition(tp.topic, tp.partition, target_ms)
               for tp in assignment]

        offsets = consumer.offsets_for_times(tps)
        for tp in offsets:
            if tp.offset >= 0:
                consumer.seek(tp)
                print(f'  Partition {tp.partition}: '
                      f'seeking to offset {tp.offset}')

        # Read events
        count = 0
        events = []

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    if count > 0:
                        break
                    continue
                if msg.error():
                    continue

                event = json.loads(msg.value().decode('utf-8'))
                events.append({
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'event': event,
                })
                count += 1

                if limit and count >= limit:
                    break
        finally:
            consumer.close()

        return events

    def replay_from_offset(self, topic, partition, offset, limit=100):
        """Replay from a specific partition and offset."""
        consumer = Consumer(self.config)
        consumer.assign([TopicPartition(topic, partition, offset)])

        events = []
        count = 0

        try:
            while count < limit:
                msg = consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    continue

                event = json.loads(msg.value().decode('utf-8'))
                events.append({
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'event': event,
                })
                count += 1
        finally:
            consumer.close()

        return events

# ============================================
# CLI Interface
# ============================================
def main():
    parser = argparse.ArgumentParser(description='StreamPulse Event Replay')
    parser.add_argument('--topic', required=True)
    parser.add_argument('--mode', choices=['beginning', 'time', 'offset'],
                       required=True)
    parser.add_argument('--hours-ago', type=float, default=1)
    parser.add_argument('--partition', type=int, default=0)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--output', default=None,
                       help='Output file for replay data')

    args = parser.parse_args()

    tool = ReplayTool(
        'localhost:9092',
        f'replay-tool-{int(time.time())}'
    )

    print(f'Replay mode: {args.mode}')

    if args.mode == 'beginning':
        events = tool.replay_from_beginning(args.topic, args.limit)
    elif args.mode == 'time':
        events = tool.replay_from_timestamp(
            args.topic, args.hours_ago, args.limit)
    elif args.mode == 'offset':
        events = tool.replay_from_offset(
            args.topic, args.partition, args.offset, args.limit)

    print(f'\nReplayed {len(events)} events')

    # Print first 5
    for e in events[:5]:
        print(f"  P{e['partition']} offset={e['offset']}: "
              f"{json.dumps(e['event'])[:80]}...")

    # Save to file if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(events, f, indent=2)
        print(f'Saved to {args.output}')

if __name__ == '__main__':
    main()
